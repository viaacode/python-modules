import logging

from .cache import LocalCacher

logging.basicConfig()
_log = logging.getLogger(__name__)
_log.propagate = True
# _log.setLevel(logging.DEBUG)
_log = _log.debug


def _get_cache_key(*args, **kwargs):
    _log(args)
    return '||'.join(('|'.join(args), '|'.join(kwargs.items())))


def memoize(f, cacher=None):
    """Usage:
    @memoize
    def someFunc():
    """
    if cacher is None:
        cacher = LocalCacher(max_items=500)

    def _cacher(*args, **kwargs):
        global _log
        x = _get_cache_key(*args, **kwargs)

        if x in cacher:
            _log('%s(%s): got: %s' % (memoize.__name__, f.__name__, str(x)))
            return cacher[x]

        res = f(*args, **kwargs)
        _log('%s(%s): set: %s' % (memoize.__name__, f.__name__, str(x)))
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
    def _cacher(*args, **kwargs):
        global _log
        cacher = args[0].get_cacher()
        if not cacher:
            cacher = cache.DummyCacher()
        x = _get_cache_key(f.__name__, *args[1:], **kwargs)

        if x in cacher:
            _log('%s(%s): got: %s' % (classcache.__name__, f.__name__, str(x)))
            return cacher[x]

        res = f(*args, **kwargs)
        _log('%s(%s): set: %s' % (classcache.__name__, f.__name__, str(x)))
        cacher[x] = res
        return res

    return _cacher


if __name__ == '__main__':
    # run with `python3 -m pythonmodules.decorators` from parent directory
    import doctest
    doctest.testmod()

