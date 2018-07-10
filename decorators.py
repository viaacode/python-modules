import logging

from . import cache

logging.basicConfig()
_log = logging.getLogger(__name__)
_log.propagate = True
_log.setLevel(logging.DEBUG)
_log = _log.debug


def memoize(f, cacher=None):
    """Usage:
    @memoize
    def someFunc():
    """
    if cacher is None:
        cacher = cache.DictCacher()

    def get_cache_key(*args, **kwargs):
        return str((args, tuple(kwargs.items())))

    def _cacher(*args, **kwargs):
        global _log
        x = get_cache_key(*args, **kwargs)
        if x in cacher:
            res = cacher[x]
            _log('%s: gotten: %s' % (memoize.__name__, f.__name__, str(x)))
        else:
            res = f(*args, **kwargs)
            _log('%s: set: %s' % (memoize.__name__, f.__name__, str(x)))
            cacher[x] = res
        return res

    return _cacher


def cache(cacher=None):
    """Usage:
    @cache(LocalCacher())
    def someFunc():

    > @cache
    > def test():
    >   print 'test'
    >   return 'result'
    >
    > test()
    test
    'result'
    > test()
    'result'
    """
    def _(f):
        return memoize(f, cacher=cacher)
    return _


def classcache(f):
    """Usage:
    class SomeClass:
        @classcache
        def someFunc(self):
    """
    def get_cache_key(*args, **kwargs):
        return args, tuple(kwargs.items())

    def _cacher(*args, **kwargs):
        global _log
        cacher = args[0].get_cacher()
        if not cacher:
            cacher = cache.DummyCacher()
        x = get_cache_key(*args[1:], **kwargs)
        if x in cacher:
            res = cacher[x]
            _log('%s(%s): get: %s' % (classcache.__name__, f.__name__, str(x)))
        else:
            res = f(*args, **kwargs)
            _log('%s(%s): set: %s' % (classcache.__name__, f.__name__, str(x)))
            cacher[x] = res
        return res

    return _cacher


if __name__ == '__main__':
    # run with `python3 -m pythonmodules.decorators` from parent directory
    import doctest
    doctest.testmod()

