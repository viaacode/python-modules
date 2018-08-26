from jsonrpc_requests import Server, ProtocolError
from .config import Config

import logging
import http.client as http_client
from urllib.parse import urlparse


class Namenlijst:
    __jsonrpc = None
    __config = None
    __token = None

    def __init__(self, config=None, log_http_requests=None):
        self.__config = Config(config, 'namenlijst')
        if type(log_http_requests) == bool:
            self.set_log_http_requests(log_http_requests)

        url = urlparse(self.__config['api_host'])
        self._user = url.username
        self._passwd = url.password
        url = url._replace(netloc="{}:{}".format(url.hostname, url.port)).geturl()
        self.__jsonrpc = Server(url)

    # def findPersonAdvanced(query = None):
    # todo

    def refresh_token(self):
        self.__token = self.__jsonrpc.authenticate(account=self._user, password=self._passwd)
        return self.__token

    @staticmethod
    def set_log_http_requests(enabled=False):
        """Toggle logging of http requests
        """
        http_client.HTTPConnection.debuglevel = 1 if enabled else 0
        log_level = logging.DEBUG if enabled else logging.WARNING

        logging.basicConfig()
        logging.getLogger().setLevel(log_level)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(log_level)
        requests_log.propagate = enabled

    def __getattr__(self, method_name):
        if self.__token is None:
            self.refresh_token()
        return Method(self, self.__jsonrpc, self.__token, method_name)
    
    def rate_match(self, name, nmlid, context):

        person = self.findPerson(document={"_id": nmlid}, limit=1)
        if not len(person):
            raise KeyError('Person with id "%s" not found', nmlid)
        person = next(person)

        # fix died_age if missing
        if person['sort_died_date'] and person['sort_born_date'] and not person['died_age']:
            person['died_age'] = int(int(person['sort_died_date'])/10000 - int(person['sort_born_date'])/10000)



class Method:
    __iterable_methods = [
        'findPerson', 'findEvent', 'findPlace', 'findMemorial', 'findMilitaryEntity',
        'getArmyList', 'getRegimentList', 'getUnitList', 'getUnitNumberList'
    ]

    def __init__(self, obj, jsonrpc, token, method_name):
        if method_name.startswith("_"):
            raise AttributeError("invalid attribute '%s'" % method_name)

        self.__obj = obj
        self.__token = token
        self.__jsonrpc = jsonrpc
        self.__method_name = method_name

    def __call__(self, *args, **kwargs):
        kwargs = dict(kwargs, token=self.__token)

        if self.__method_name in self.__iterable_methods:
            kwargs['total'] = 'true'
            result = ResultIterator(getattr(self.__jsonrpc, self.__method_name), kwargs)
            return result

        try:
            result = getattr(self.__jsonrpc, self.__method_name)(kwargs)
        except ProtocolError:
            kwargs['token'] = self.__obj.refresh_token()
            result = getattr(self.__jsonrpc, self.__method_name)(kwargs)

        return result


class ResultIterator:
    def __init__(self, method, kwargs):
        self.buffer_size = kwargs['limit'] if 'limit' in kwargs else 25
        self.method = method
        self.kwargs = kwargs
        self.length = None
        self.buffer = []
        self.i = kwargs['skip'] if 'skip' in kwargs else 0
        self.buffer_idx = 0

    def __iter__(self):
        return self

    def fetch_next(self):
        self.kwargs['limit'] = self.buffer_size
        self.kwargs['skip'] = self.i
        results = self.method(self.kwargs)
        self.length = results['total']
        self.buffer = results['data']

    def __len__(self):
        if self.length is None:
            self.fetch_next()
        return self.length

    def __next__(self):
        if self.length is None:
            self.fetch_next()

        if self.i >= self.length:
            raise StopIteration()

        self.i += 1
        self.buffer_idx += 1

        if self.buffer_idx >= len(self.buffer) and self.i < self.length:
            self.buffer_idx = 0
            self.fetch_next()

        return self.buffer[self.buffer_idx - 1]

    def set_buffer_size(self, buffer_size):
        self.buffer_size = buffer_size

# class AdvancedResultIterator(ResultIterator):
# TODO
#    def fetch_next(self):
#        self.kwargs['limit'] = self.buffer_size
#        self.kwargs['skip'] = self.i
#        results = self.method(self.kwargs)
#        self.length = results['total']
#        self.buffer = results['data']
