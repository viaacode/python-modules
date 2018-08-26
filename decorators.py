import logging

from .cache import LocalCacher

_log = logging.getLogger(__name__)
# _log.propagate = True
# _log.setLevel(logging.DEBUG)
_log = _log.debug


def _get_cache_key(*args, **kwargs):
    _log(args)
    return '||'.join(('|'.join(args), '|'.join(kwargs.items())))


def log_call(logger: logging.Logger, log_level=logging.DEBUG):
    """
    Decorator to log all calls to decorated function to given logger

    >>> import logging, sys
    >>> logging.basicConfig(stream=sys.stdout, format='%(levelname)s:%(name)s: %(message)s')
    >>> logger = logging.getLogger('logger_name')
    >>> logger.setLevel(logging.DEBUG)
    >>> @log_call(logger, logging.WARNING)
    ... def test(*args, **kwargs):
    ...     pass
    >>> test('arg1', arg2='someval', arg3='someotherval')
    WARNING:logger_name: test(arg1, 'arg2': 'someval', 'arg3': 'someotherval')
    """
    def _log_call(func: callable):
        def _(*args, **kwargs):
            logger.log(log_level, '%s(%s, %s)', func.__name__, ', '.join(args), [k + '=' + str(kwargs[k]) for k in kwargs])
            return func(*args, **kwargs)
        return _
    return _log_call


def exception_redirect(new_exception_class, old_exception_class=Exception, logger=None):
    """
    Decorator to replace a given exception to another Exception class, with optional exception logging.

    >>>
    >>> class MyException(Exception):
    ...     pass
    >>>
    >>> @exception_redirect(MyException)
    ... def test():
    ...    raise Exception("test")
    >>>
    >>> test()
    Traceback (most recent call last):
    ...
    MyException: test
    """
    def _decorator(func):
        def catch_and_redirect_exception(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except old_exception_class as e:
                if logger is not None:
                    logger.exception(e)
                raise new_exception_class(e) from None
        return catch_and_redirect_exception
    return _decorator


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

