from sqlalchemy import MetaData, create_engine
from sqlalchemy.sql.schema import Table
from . import obfuscate_password_from_url
from pythonmodules.profiling import timeit


class ReflectDB:
    """
    None-optimal sqlalchemy db helper object (using reflection to build ORM)
    """
    def __init__(self, connection_url):
        self._db = None
        self._meta = None
        self._connected = False
        self._connection_url = connection_url

    def connect(self):
        if self._connected:
            return
        self._db = create_engine(self._connection_url)
        dburl = obfuscate_password_from_url(self._connection_url)
        with timeit('connect db %s' % dburl, 1000):
            self._db.connect()

        self._connected = True
        with timeit('reflect db %s' % dburl, 1000):
            self._meta = MetaData(bind=self._db)
            self._meta.reflect()

    @property
    def db(self):
        self.connect()
        return self._db

    @property
    def meta(self):
        self.connect()
        return self._meta

    def __getattr__(self, item) -> Table:
        return self.meta.tables[item]

    def __repr__(self):
        return '<%s:%s>' % (type(self), obfuscate_password_from_url(self._connection_url))

    def execute(self, *args, **kwargs):
        return self.db.execute(*args, **kwargs)
