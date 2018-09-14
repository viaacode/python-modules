from queue import Queue
from threading import Thread
from functools import partial

import logging
logger = logging.getLogger(__name__)


class MultiThreadError(BaseException):
    pass


class AlreadyRunningError(MultiThreadError):
    pass


class MultiThread:
    """
    Simple wrapper class for basic multithreading

    >>> import time
    >>> from collections import namedtuple
    >>> def noop(*args, **kwargs):
    ...    pass
    >>> def refl(*args, **kwargs):
    ...    return args, kwargs
    >>> t = MultiThread(noop, n_workers=10)
    >>> t.extend(range(0, 1000))
    >>> t.run()
    []
    >>> total = 0
    >>> n_workers = 3
    >>> workers = [0] * n_workers
    >>> def proc(n, thread_id, *args, **kwargs):
    ...    global total, workers
    ...    time.sleep(.01)
    ...    workers[thread_id] += 1
    ...    total += n
    ...    return n
    ...
    >>> t = MultiThread(proc, n_workers=n_workers)
    >>> t.extend(range(0, 100))
    >>> results = t.run()
    >>> set(results) == set(range(0, 100))
    True
    >>> total
    4950
    >>> sum(workers)
    100
    >>> 0 in workers
    False
    >>> t = MultiThread(refl, n_workers=2)
    >>> t.extend(range(0, 10))
    >>> res = t.run('test', 'arg2', kw=True)
    >>> len(res)
    10
    >>> all(len(a) == 3 for a, k in res)
    True
    >>> all(a[0] == 'test' for a, k in res)
    True
    >>> all(a[1] == 'arg2' for a, k in res)
    True
    >>> all(a[2] in range(0, 10) for a, k in res)
    True
    >>> all(k['kw'] is True for a, k in res)
    True
    >>> all(k['thread_id'] in [0, 1] for a, k in res)
    True
    >>> t = MultiThread(lambda k, **kwargs: '%s%.s' % (k, time.sleep(.2)))
    >>> t.append('before')
    >>> t.result
    >>> t.start()
    >>> t.result
    []
    >>> time.sleep(.2)
    >>> t.result
    ['before']
    >>> t.append('after')
    >>> t.result
    ['before']
    >>> t.wait() #doctest: +ELLIPSIS
    <__main__.MultiThread object at 0x...>
    >>> t.result
    ['before', 'after']
    """
    def __init__(self, processor=None, n_workers=5, pbar=None):
        self.processor = processor
        self.q = Queue()
        self.n_workers = n_workers
        self.pbar = pbar
        self.logger = logger
        self.result = None

    def _worker(self, *args, **kwargs):
        processor = partial(self.processor, *args, **kwargs)
        while True:
            try:
                args = self.q.get()
                result = processor(*args)
                if result is not None:
                    self.result.append(result)
                self.q.task_done()
                if self.pbar is not None:
                    self.pbar.update(1)
            except Exception as e:
                if self.logger:
                    self.logger.exception(e)

    def append(self, *args):
        self.q.put(args)

    def extend(self, iterable):
        for row in iterable:
            self.append(row)

    def wait(self):
        self.q.join()
        return self

    def start(self, *args, **kwargs):
        if self.running:
            raise AlreadyRunningError("Attempted to run while already running")
        self.result = []
        for i in range(self.n_workers):
            kwargs['thread_id'] = i
            t = Thread(target=partial(self._worker, *args, **kwargs), daemon=True)
            t.start()

    @property
    def running(self):
        return self.result is not None

    def run(self, *args, **kwargs) -> list:
        self.start(*args, **kwargs)
        self.wait()
        result = self.result
        self.result = None
        return result


def multithreaded(*args, **kwargs):
    """
    Decorator version for multithreading

    >>> import time
    >>> from collections import namedtuple
    >>> @multithreaded(2)
    ... def refl(*args, **kwargs):
    ...    return args, kwargs
    >>> @multithreaded(10)
    ... def proc1(*args, **kwargs):
    ...   pass
    >>> proc1(range(0, 100))
    []
    >>> total = 0
    >>> n_workers = 3
    >>> workers = [0] * n_workers
    >>>
    >>> @multithreaded(n_workers)
    ... def proc(n, thread_id, *args, **kwargs):
    ...    global total, workers
    ...    time.sleep(.01)
    ...    workers[thread_id] += 1
    ...    total += n
    ...    return n
    ...
    >>> results = proc(range(0, 100))
    >>> set(results) == set(range(0, 100))
    True
    >>> total
    4950
    >>> sum(workers)
    100
    >>> 0 in workers
    False
    >>> res = refl(range(0, 10), 'test', 'arg2', kw=True)
    >>> len(res)
    10
    >>> all(len(a) == 3 for a, k in res)
    True
    >>> all(a[0] == 'test' for a, k in res)
    True
    >>> all(a[1] == 'arg2' for a, k in res)
    True
    >>> all(a[2] in range(0, 10) for a, k in res)
    True
    >>> all(k['kw'] is True for a, k in res)
    True
    >>> all(k['thread_id'] in [0, 1] for a, k in res)
    True
    """

    # Add 'None' processor
    a = [None]
    a.extend(args)
    args = a
    mt = MultiThread(*args, **kwargs)

    def _decorator(func):
        mt.processor = func

        def _(alist, *args, **kwargs):
            mt.extend(alist)
            return mt.run(*args, **kwargs)
        return _

    return _decorator


if __name__ == "__main__":
    import doctest
    doctest.testmod()

