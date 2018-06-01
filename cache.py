import collections


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

    def __init__(self, obj):
        self.obj = obj

    def __setitem__(self, k, v):
        return self.obj.set(k, v)

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
    max_items = None
    dict = collections.OrderedDict()

    def __init__(self, max_items=None):
        self.max_items = max_items
        self.dict = collections.OrderedDict()

    def __setitem__(self, k, v):
        if self.max_items is not None and len(self.dict) >= self.max_items:
            self.dict.popitem(last=False)
        self.dict[str(k)] = v

    def __getitem__(self, k):
        return self.dict[str(k)]

    def __contains__(self, k):
        return str(k) in self.dict


class NullCacher:
    @staticmethod
    def __getitem__(self, item):
        return None

    @staticmethod
    def __setitem__(self, key, value):
        return False

    @staticmethod
    def __contains__(*args):
        return False


if __name__ == '__main__':
    import doctest
    doctest.testmod()
