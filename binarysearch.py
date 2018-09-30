import math
import os
from .multithreading import multithreaded
from tqdm import tqdm
from abc import ABC

import logging
logger = logging.getLogger(__name__)


class Chunk:
    r"""
    Class representing a 32 bit int (as bytes)

    >>> Chunk.from_int(0)
    <Chunk:b'\x00\x00\x00\x00'>
    >>> Chunk(b"\x00\x00\x00\x00") == Chunk.from_int(0)
    True
    >>> Chunk(b"\x01\x00\x00\x00").__int__()
    1
    >>> Chunk(b"\x00\x00\x00\x01").__int__()
    16777216
    >>> Chunk(b"\xff\xff\xff\xff").__int__()
    4294967295
    >>> Chunk(b"\xfe\xff\xff\xff").__int__()
    4294967294
    """
    def __init__(self, data: bytes):
        self.data = data

    def __int__(self):
        return int.from_bytes(self.data, 'little')

    @classmethod
    def from_int(cls, n, length=4):
        return cls(n.to_bytes(length, 'little'))

    def __eq__(self, other):
        return self.data == other.data

    def __ne__(self, other):
        return self.data != other.data

    def __lt__(self, other):
        return self.data < other.data

    def __le__(self, other):
        return self.data <= other.data

    def __gt__(self, other):
        return self.data > other.data

    def __ge__(self, other):
        return self.data >= other.data

    def __repr__(self):
        return '<%s:%a>' % (type(self).__name__, self.data)


# deprecated, this is a slower recursive version (with debug)
def binary_search_recursive(alist, item, start=None, stop=None, debugger=None):
    """
    Recursive version with debug (slower)

    :return: int|None

    >>> binary_search_recursive([4], 4)
    0
    >>> n = 10
    >>> all(binary_search_recursive(range(0, n), i) is not None for i in range(0, n))
    True
    >>> any(binary_search_recursive(range(0, n), i) is not None for i in range(n, 3*n))
    False
    >>> sum(binary_search_recursive(range(0, n), i) for i in range(0, n))
    45
    """
    # debugger = logging.getLogger(__name__)
    # debugger.setLevel(logging.DEBUG)
    if not len(alist):
        return None

    if start is None:
        start = 0
    if stop is None:
        stop = len(alist) - 1

    if start < 0:
        return None

    mid = (stop + start) // 2
    found = alist[mid]

    if debugger is not None:
        w = 50
        msg = list('░' * math.floor(w * start / len(alist)))
        msg.append('█' * math.floor(w * (stop - start) / len(alist)))
        msg.append('░' * math.floor(w * ((len(alist) - stop) / len(alist))))
        msg.append(' looking for %a (vs. %a) in %d -> %d (mid %d)' %
                   (item, found, start, stop, mid))
        debugger.debug(''.join(msg))

    if found == item:
        return mid

    if start == stop:
        return None

    if item < found:
        return binary_search_recursive(alist, item, start, mid)

    if mid + 1 > stop:
        return None

    return binary_search_recursive(alist, item, mid + 1, stop)


def binary_search(alist, item, start=None, stop=None):
    """
    :return: int|None

    >>> binary_search([], 't') is None
    True
    >>> binary_search([3, 4, 5], 4)
    1
    >>> n = 10
    >>> [binary_search(range(0, n), i) for i in range(0, n)]
    [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]
    >>> all(binary_search(range(0, n), i) is not None for i in range(0, n))
    True
    >>> any(binary_search(range(0, n), i) is not None for i in range(n, 3*n))
    False
    >>> sum(binary_search(range(0, n), i) for i in range(0, n))
    45
    """
    if not len(alist):
        return None

    if start is None:
        start = 0
    if stop is None:
        stop = len(alist) - 1

    if start < 0:
        return None

    while start < stop:
        mid = (stop + start) // 2
        found = alist[mid]
        if found < item:
            start = mid + 1
        else:
            stop = mid

    if alist[start] == item:
        return start

    return None


class EmptySortedBytes:
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

    def __len__(self):
        return 0

    def __contains__(self, item):
        return False

    def __repr__(self):
        return '<%s>' % type(self).__name__

    def __getitem__(self, item: int):
        raise IndexError

    def __call__(self, *args, **kwargs):
        return self


# sorta singleton
EmptySortedBytes = EmptySortedBytes()


class ASortedBytes(ABC):
    def assert_valid_item(self, item):
        if len(item) != self._nbytes:
            raise KeyError("Expected length of %s" % self._nbytes)

    def __contains__(self, item):
        self.assert_valid_item(item)
        return binary_search(self, item) is not None

    def __enter__(self):
        return self

    def __exit__(self):
        pass


class SortedBytesFile(ASortedBytes):
    """
    Reads the 32-bit int files as proper chunks, returns as bytes
    """
    def __init__(self, filename, nbytes=4):
        self._filename = filename
        self._file = None
        self._len = 0
        self._nbytes = nbytes

    def __enter__(self):
        size = os.stat(self._filename).st_size
        if size % self._nbytes:
            raise EOFError("Expected size of multiple of %sB" % self._nbytes)
        if size == 0:
            # optim if file is empty
            return EmptySortedBytes()
        self._len = size // self._nbytes
        self._file = open(self._filename, 'rb')
        return self

    def __getitem__(self, item):
        if type(item) is slice:
            if item.stop or item.step:
                raise NotImplementedError("Slice not supported atm")
            item = item.start
        if type(item) is not int:
            raise NotImplementedError("Only supporting integers as item keys")
        if item < 0:
            item = self._len + item
        if item < 0 or item >= self._len:
            raise IndexError("Index %d out of range" % item)
        self._file.seek(item * self._nbytes)
        return self._file.read(self._nbytes)

    def __len__(self) -> int:
        return self._len

    def __repr__(self):
        return '<%s %s[%d]>' % (type(self).__name__, self._filename, self._len)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file:
            self._file.close()


class CachedSortedBytesFile:
    def __init__(self, filename):
        self._filename = filename

    def __enter__(self):
        with open(self._filename, 'r') as file:
            data = file.read()
        return SortedBytesMemory(data)


class SortedBytesMemory(ASortedBytes):
    def __init__(self, data, nbytes=4):
        size = len(data)
        if size % nbytes:
            raise KeyError("File length needs to be multiple of %d" % nbytes)

        self._len = size // nbytes
        self._data = data
        self._nbytes = nbytes

    def __getitem__(self, item):
        if type(item) is slice:
            if item.stop or item.step:
                raise NotImplementedError("Slice not supported atm")
            item = item.start
        if type(item) is not int:
            raise NotImplementedError("Only supporting integers as item keys")
        if item < 0:
            item = self._len + item
        if item < 0 or item >= self._len:
            raise IndexError("Index %d out of range" % item)
        return self._data[item * self._nbytes]


class SortedChunksFile(SortedBytesFile):
    """
    Reads the 32-bit int files as proper chunks, returns as Chunk
    """
    def __getitem__(self, item: int):
        return Chunk(super().__getitem__(item))

    def assert_valid_item(self, item):
        if type(item) is not Chunk:
            raise KeyError("Expected a Chunk")


def file_contains(file, checksums):
    with SortedBytesFile(file) as reader:
        return all(checksum in reader for checksum in checksums)


class SortedBytesDirectory:
    def __init__(self, path, suffix=None):
        self._path = os.fsencode(path)
        if suffix is None:
            suffix = '.crc'
        self._suffix = suffix

    def basename(self, file):
        return os.path.basename(file)[:-len(self._suffix)]

    def files(self):
        prefix = None
        if os.path.isfile(self._path):
            files = [self._path]
        else:
            prefix = self._path
            files = os.listdir(self._path)

        for file in files:
            if prefix:
                file = os.path.join(prefix, file)

            filename = os.fsdecode(file)
            if filename.endswith('.crc'):
                yield filename

    def search(self, checksums):
        files = list(self.files())
        for file in tqdm(files):
            if file_contains(file, checksums):
                yield file

    def search_multithread(self, checksums, threads=5):
        files = list(self.files())
        pbar = tqdm(total=len(files))

        @multithreaded(threads, pbar=pbar)
        def lookup(checksums_, file, *args, **kwargs):
            if file_contains(file, checksums_):
                return file

        return lookup(files, checksums)


if __name__ == "__main__":
    # run with `python3 -m pythonmodules.binarysearch` from parent directory
    import doctest
    doctest.testmod()
