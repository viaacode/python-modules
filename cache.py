import collections
import logging

logger = logging.getLogger(__name__)


class DictCacher(dict):
    """Simple 'Local' cacher using a new dict... Usable to re-use same interface
       for other classes...

    >>> cache = DictCacher()
    >>> cache['test'] = True
    >>> cache['test']
    True
    >>> 'test' in cache
    True
    >>> cache['test2'] = 2
    >>> 'test2' in cache
    True
    >>> cache['test3'] = 'three'
    >>> 'test3' in cache
    True
    >>> cache['test3']
    'three'
    >>> 'test' in cache
    True
    """
    pass


class WrapperCacher:
    """Wrapper class for cache classes that use .get, .set and .has_key methods instead of item assignments,
    eg. django FileBasedClass
    """
    def __init__(self, obj,  timeout=None, version=None):
        self.obj = obj
        self.extra_write_arguments = []
        if timeout is not None:
            self.extra_write_arguments.append(timeout)
        if version is not None:
            self.extra_write_arguments.append(version)

    def __setitem__(self, k, v):
        args = [k, v]
        args.extend(self.extra_write_arguments)
        return self.obj.set(*args)

    def __getitem__(self, k):
        return self.obj.get(k)

    def __contains__(self, k):
        return self.obj.has_key(k)


class LocalCacher:
    """Simple 'Local' cacher with a maximum amount of items

    >>> cache = LocalCacher(2)
    >>> cache.max_items
    2
    >>> cache['test'] = True
    >>> cache['test']
    True
    >>> 'test' in cache
    True
    >>> cache['test2'] = 2
    >>> 'test2' in cache
    True
    >>> cache['test3'] = 'three'
    >>> 'test3' in cache
    True
    >>> cache['test3']
    'three'
    >>> 'test' in cache
    False
    >>> cache['test']
    Traceback (most recent call last):
    ...
    KeyError: 'test'
    """
    def __init__(self, max_items=None):
        self.dict = collections.OrderedDict()
        self.max_items = max_items

    def __setitem__(self, k, v):
        if self.max_items is not None and len(self.dict) >= self.max_items:
            self.dict.popitem(last=False)
        self.dict[str(k)] = v

    def __getitem__(self, k):
        return self.dict[str(k)]

    def __contains__(self, k):
        return str(k) in self.dict


class DummyCacher:
    """
    >>> cache = DummyCacher()
    >>> cache['test'] = True
    >>> print(cache['test'])
    None
    >>> 'test' in cache
    False
    >>> cache['test2'] = 2
    >>> 'test2' in cache
    False
    """
    @staticmethod
    def __getitem__(i):
        return None

    @staticmethod
    def __setitem__(k, v):
        return False

    @staticmethod
    def __contains__(k):
        return False


if __name__ == '__main__':
    import doctest
    doctest.testmod()
