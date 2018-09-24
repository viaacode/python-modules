from pythonmodules.config import Config
import logging
from binascii import crc32
from itertools import chain
from tqdm import tqdm
import os
from pythonmodules.profiling import timeit
from pythonmodules.binarysearch import SortedBytesDirectory, SortedBytesFile
from collections import namedtuple, deque
from abc import ABC, abstractmethod
import gzip
import pysolr
from pythonmodules.db import ReflectDB
from sqlalchemy.sql.expression import func, select
import pysolr

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


class WordSearcherSolr:
    def __init__(self, url):
        self._url = url

    def search(self, words):
        solr = pysolr.Solr(self._url, timeout=10)
        # q = 'text:(%s)' % (' AND '.join(words))
        q = 'text:"%s"' % (' '.join(words),)
        return solr.search(q)


# class WordSearcherBinarySearch:
#     def __init__(self, path):
#         if not os.path.exists(path):
#             raise WordSearcherError("Path '%s' does not exist" % path)
#         self._path = os.path.realpath(path)
#         self.searcher = SortedBytesDirectory(self._path)
#
#     @property
#     def path(self):
#         return self._path
#
#     @staticmethod
#     def hasher(b):
#         return crc32(b.encode('ascii')).to_bytes(4, 'little')
#
#     def search(self, words, threads=7):
#         if threads is None or threads == 1:
#             return self.searcher.search(words)
#
#         return self.searcher.search_multithread(words, threads=threads)


class WordSearcher:
    def __init__(self, config=None):
        config = Config(config, section='wordsearcher')
        self._strat = 'solr' if 'strategy' not in config else config['strategy']
        if self._start == 'solr':
            self._searcher = WordSearcherSolr(config)
        # elif self._start == 'binarysearch':
        #     self._searcher = WordSearcherBinarySearch('./indexes')
        self._config = config

    def search(self, words):
        return self._searcher.search(words)


class WordSearcherAdmin(WordSearcher):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = ReflectDB(self._config['db'])

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




