from pythonmodules.config import Config
from sqlalchemy import MetaData, create_engine
from sqlalchemy.sql.expression import func, select
import logging
from binascii import crc32
from itertools import chain
from tqdm import tqdm
import os
from pythonmodules.profiling import timeit
from pythonmodules.binarysearch import SortedBytesDirectory, SortedBytesFile
from collections import namedtuple, deque
from abc import ABC, abstractmethod
from sqlalchemy.sql.schema import Table
import gzip

logger = logging.getLogger(__name__)


class WordSearcherError(BaseException):
    pass


ProfilingResult = namedtuple('ProfilingResult', ['threads', 'results', 'duration', 'bytes'])


# ProfilingResultCallback classes

class AProfilingResultCallback(ABC):
    @abstractmethod
    def __call__(self, result: ProfilingResult):
        pass

    @staticmethod
    def format_bytes(b):
        bytesf = '%02X' * len(b[0]) * len(b)
        return bytesf % (*chain(*b),)


class ProfilingResultNullCallback(AProfilingResultCallback):
    def __call__(self, result: ProfilingResult):
        pass


class ProfilingResultCsvCallback(AProfilingResultCallback):
    def __init__(self, file):
        file = os.path.realpath(file)
        if not os.path.isfile(file):
            with open(file, 'a') as csv:
                csv.write(','.join(ProfilingResult._fields) + "\n")
        self._filename = file

    def __call__(self, result: ProfilingResult):
        text = '%d,%d,%d,%s\n' % (result.threads, result.results, result.duration, self.format_bytes(result.bytes))
        with open(self._filename, 'a') as csv:
            csv.write(text)


class ProfilingResultPrintCallback(AProfilingResultCallback):
    def __call__(self, result: ProfilingResult):
        print(result)


class ProfilingResultAggregatorCallback(AProfilingResultCallback, deque):
    def __call__(self, result: ProfilingResult):
        for cb in self:
            cb(result)


class WordSearcher:
    def __init__(self, path):
        if not os.path.exists(path):
            raise WordSearcherError("Path '%s' does not exist" % path)
        self._path = os.path.realpath(path)
        self.searcher = SortedBytesDirectory(self._path)

    @property
    def path(self):
        return self._path

    @staticmethod
    def hasher(b):
        return crc32(b.encode('ascii')).to_bytes(4, 'little')

    def search(self, words, threads=7):
        if threads is None or threads == 1:
            return self.searcher.search(words)

        return self.searcher.search_multithread(words, threads=threads)


class DB:
    def __init__(self, *args, **kwargs):
        self._db = None
        self._meta = None
        self._connected = False
        super().__init__(*args, **kwargs)

    def connect(self):
        if self._connected:
            return
        config = Config()
        self._db = create_engine(config['db']['connection_url_live'])
        with timeit('connect db', 1000):
            self._db.connect()

        self._connected = True
        with timeit('reflect db', 1000):
            self._meta = MetaData(bind=self._db)
            self._meta.reflect()

    @property
    def db(self):
        self.connect()
        return self._db

    @property
    def meta(self):
        self.connect()
        return self._meta

    def __getattr__(self, item):
        return self.get_table(item)

    def get_table(self, table) -> Table:
        return self.meta.tables[table]

    def execute(self, *args, **kwargs):
        return self.db.execute(*args, **kwargs)


class WordSearcherAdmin(WordSearcher):
    def __init__(self, *args, **kwargs):
        self._db = None
        super().__init__(*args, **kwargs)

    @property
    def db(self):
        if self._db is None:
            self._db = DB()
        return self._db
    
    def build_index(self, prefix='', suffix='.crc', skip_if_exists=True):
        table = self.db.attestation_texts
        c = next(func.max(table.c.id).select().execute())[0]
        logger.debug('Build index: %d items to process', c)
        with tqdm(total=c) as pbar:
            for row in table.select().execution_options(stream_results=True).execute():
                pid = row[1]
                filename = os.path.join(self.path, "%s%s%s" % (prefix, pid, suffix))
                if skip_if_exists and os.path.isfile(filename):
                    pbar.set_description('%s already exists' % pid)
                    pbar.update(1)
                    logger.debug('%s already exists: skip', pid)
                    continue
                pbar.set_description(pid)
                words = sorted(set(map(self.hasher, filter(len, row[2].split(' ')))))
                with open(filename, 'wb') as f:
                    f.write(b''.join(words))
                logger.debug('pid %s done, %d words witten to: %s', (pid, len(words), filename))
                pbar.update(1)

    def build_pid_map(self):
        table = self.db.attestation_texts
        mapping = dict()
        size = self.db.execute(func.max(table.c.id)).scalar()
        pids = select([table.c.id, table.c.pid]).execution_options(stream_results=True).execute()
        for id_, pid in tqdm(pids, total=size):
            mapping[id_.to_bytes(4, byteorder='little')] = pid

        with open(os.path.join(self.path, 'pidmap.py'), 'w+') as f:
            f.write('id2pid = ')
            f.write(str(mapping))
            f.write('\n')
            f.write('pid2id = ')
            f.write(str({v: k for k, v in mapping.items()}))

    def build_reverse_index(self):
        from .multithreading import multithreaded
        import resource
        from indexes.pidmap import pid2id

        def pid2bytes(pid):
            if pid not in pid2id:
                raise WordSearcherError("Unmapped pid '%s'" % pid)
            return pid2id[pid]

        # make sure directory structure exists ok
        hex = '0123456789ABCDEF'
        wordfiletpl = './revindexes/%s'
        for a in hex:
            if not os.path.isdir(wordfiletpl % a):
                os.mkdir(wordfiletpl % a, 0o770)
            for b in hex:
                if not os.path.isdir(wordfiletpl % (os.path.join(a, b))):
                    os.mkdir(wordfiletpl % (os.path.join(a, b)), 0o770)

        max_open_files = resource.getrlimit(resource.RLIMIT_NOFILE)[0] * 7 // 100
        # limits[0] = resource.RLIM_INFINITY
        # limits[1] = resource.RLIM_INFINITY
        # limits = (resource.RLIM_INFINITY, resource.RLIM_INFINITY)
        # limits = (500000, 1000000)
        # resource.setrlimit(resource.RLIMIT_NOFILE, limits)

        def evict(k, v):
            v.close()

        # filecache = LRU(max_open_files, callback=evict)

        files = list(self.searcher.files())

        @multithreaded(1, pbar=tqdm(total=len(files)))
        def dowork(file, thread_id):
            pidval = self.searcher.basename(file)

            try:
                id_bytes = pid2bytes(pidval)
            except WordSearcherError as e:
                logger.exception(e)
                return

            with SortedBytesFile(file) as words:
                for word in words:
                    getfile(word).write(id_bytes)

        def getfile(word):
            # if False and word in filecache:
            #     return filecache[word]
            wordfiletpl = './revindexes/%s'
            word = AProfilingResultCallback.format_bytes([word])

            filename = wordfiletpl % (os.path.join(word[0], word[1], word[2:] + '.rev.gz'))
            return gzip.open(filename, 'ab')

        dowork(files)

    def do_profiling(self, threads=None, callback: AProfilingResultCallback=None):
        """
        Do some profiling of the wordsearcher.

        :param threads: list|None
        :param callback: AProfilingResultCallback|callable
        :return: None
        """
        if callback is None:
            callback = ProfilingResultCsvCallback('profiling.csv')

        threads = map(int, threads) if len(threads) else [5, 10, 15]

        tests = [b'\xff\xf1\x4e\xf9',
                 b'x\xd4m\x0f',
                 b'x\xe9|\xa5',
                 b'P\x17\xb3\xb5']

        tests = [[tests[0]], [tests[3]], [tests[1], tests[2], tests[3]]]

        timer = timeit()
        for thread in threads:
            for test in tests:
                logger.info("Profiling with %d threads and data %s" %
                            (thread, AProfilingResultCallback.format_bytes(test)))
                timer.restart()
                results = self.search(test, threads=thread)
                result = ProfilingResult(thread, len(list(results)), timer.elapsed(), test)
                callback(result)

    def import_solr(self, offset=None):
        import pysolr
        table = self.db.attestation_texts

        size = self.db.execute(func.max(table.c.id)).scalar()
        res = table.select()
        if offset is not None:
            res = res.offset(offset)
        res = res.execution_options(stream_results=True).execute()

        solr_url = 'http://localhost:8983/solr/wordsearcher/'
        solr = pysolr.Solr(solr_url, timeout=10)
        batchSize = 1000
        batch = deque()
        for row in tqdm(res, total=size):
            batch.append({"id": row.pid, "text": row.text})
            if len(batch) >= batchSize:
                solr.add(batch)
                batch = []



