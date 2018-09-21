from urllib.parse import urlparse, ParseResult


def obfuscate_password_from_url(url):
    """
    Obfuscate the password in a URL
    :param url: str|ParseResult
    :return: string

    >>> url = 'https://username:%s@host.ext/testr?fsgfd=w+5&test=2#hash'
    >>> obfuscate_password_from_url(url % 'password')
    'https://username:********@host.ext/testr?fsgfd=w+5&test=2#hash'
    >>> obfuscate_password_from_url(url % '')
    'https://username:***@host.ext/testr?fsgfd=w+5&test=2#hash'
    >>> obfuscate_password_from_url(url % '____*')
    'https://username:*****@host.ext/testr?fsgfd=w+5&test=2#hash'
    >>> obfuscate_password_from_url(urlparse(url % 'somesuperlongpassword'))
    'https://username:********@host.ext/testr?fsgfd=w+5&test=2#hash'
    """
    if type(url) is not ParseResult:
        url = urlparse(url)
    passwd = "*" * max(3, min(8, len(url.password)))
    replaced = url._replace(netloc="{}:{}@{}".format(url.username, "*" * len(passwd), url.hostname))
    return replaced.geturl()


if __name__ == '__main__':
    # run with `python3 -m pythonmodules.__init__` from parent directory
    import doctest
    doctest.testmod()
