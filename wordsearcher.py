from pythonmodules.config import Config


from sqlalchemy import MetaData, create_engine
from sqlalchemy.sql.expression import func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session, sessionmaker
import logging
from pythonmodules.multithreading import MultiThread, multithreaded
from binascii import crc32
from itertools import chain
from tqdm import tqdm
import math
import os
from sys import argv
from pythonmodules.profiling import timeit
from pythonmodules.binarysearch import SortedBytesDirectory
from collections import namedtuple
from abc import ABC, abstractmethod
from argparse import ArgumentParser

logger = logging.getLogger(__name__)


class WordSearcherError(BaseException):
    pass


ProfilingResult = namedtuple('ProfilingResult', ['threads', 'results', 'duration', 'bytes'])


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


class WordSearcherAdmin(WordSearcher):
    def build_index(self, prefix='', suffix='.crc', skip_if_exists=True):
        directory = self.path
        config = Config()

        db = create_engine(config['db']['connection_url_live'])
        db.connect()

        meta = MetaData(db, reflect=True)
        table = meta.tables['attestation_texts']
        c = next(func.max(table.c.id).select().execute())[0]
        logger.debug('Build index: %d items to process', c)
        with tqdm(total=c) as pbar:
            for row in table.select().execution_options(stream_results=True).execute():
                pid = row[1]
                filename = os.path.join(directory, "%s%s%s" % (prefix, pid, suffix))
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
                timer.restart()
                results = self.search(test, threads=thread)
                result = ProfilingResult(thread, len(list(results)), timer.elapsed(), test)
                callback(result)

