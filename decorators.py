class DictCacher:
    '''Simple 'Local' cacher using a new dict... Usable to re-use same interface
       for other classes...
    '''
    def get(self, k):
        return getattr(self, str(k))
    def set(self, k, v, *args, **kwargs):
        return setattr(self, str(k), v)
    def has_key(self, k, *args, **kwargs):
        return hasattr(self, str(k))

class NullCacher:
    def get(self, k):
        return None
    def set(self, k, v, *args, **kwargs):
        return False
    def has_key(self, k, *args, **kwargs):
        return False

def memoize(f, cacher = None, timeout = None):
    '''Usage:
    @memoize
    def someFunc():
    '''
    if cacher is None:
        cacher = DictCacher()

    def get_cache_key(*args, **kwargs):
        return (args, tuple(kwargs.items()))

    def _cacher(*args, **kwargs):
        x = get_cache_key(*args, **kwargs)
        if cacher.has_key(x):
            res = cacher.get(x)
            print(__name__ + ': get: ' + str(x))
        else:
            res = f(*args, **kwargs)
            print(__name__ + ': set: ' + str(x))
            cacher.set(x, res, timeout)
        print(res)
        return res

    return _cacher

def cache(timeout = None, cacher = None):
    '''Usage:
    @cache(3600)
    def someFunc():
    '''
    def _(f):
        return memoize(f, cacher = cacher, timeout = timeout)
    return _

def classcache(f):
    '''Usage:
    class SomeClass:
        @classcache
        def someFunc(self):
    '''
    def get_cache_key(*args, **kwargs):
        return (args, tuple(kwargs.items()))

    def _cacher(*args, **kwargs):
        cacher = args[0].get_cache()
        if not cacher:
            cacher = NullCacher()
        x = get_cache_key(*args[1:], **kwargs)
        if cacher.has_key(x):
            res = cacher.get(x)
        else:
            res = f(*args, **kwargs)
            print(__name__ + ': set: ' + str(x))
            cacher.set(x, res)
        return res

    return _cacher
