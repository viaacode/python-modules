import requests
from jsonrpc_requests import Server, ProtocolError
from configparser import ConfigParser

import logging
import http.client as http_client

class Namenlijst:
    __jsonrpc = None
    __config  = None
    __token   = None

    def __init__(self, config = None, log_http_requests = None):
        if config == None:
            config = 'config.ini'
        if type(config) == str:
            config = ConfigParser()
            config.read('config.ini')
        try:
            config = config['namenlijst']
        except Exception:
            pass

        if type(log_http_requests) == bool:
            self.set_log_http_requests(log_http_requests)

        self.__config = config
        self.__jsonrpc = Server(config['api_host'])

    def refresh_token(self):
        self.__token = self.__jsonrpc.authenticate(account = self.__config['api_user'], password = self.__config['api_pass'])
        return self.__token

    def set_log_http_requests(self, enabled = False):
        """Toggle logging of http requests
        """
        http_client.HTTPConnection.debuglevel = 1 if enabled else 0
        logLevel = logging.DEBUG if enabled else logging.WARNING

        logging.basicConfig()
        logging.getLogger().setLevel(logLevel)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logLevel)
        requests_log.propagate = enabled

    def __getattr__(self, method_name):
        if (self.__token == None):
            self.refresh_token()
        return Method(self, self.__jsonrpc, self.__token, method_name)

class Method:
    __iterable_methods = [
        'findPerson', 'findEvent', 'findPlace', 'findMemorial', 'findMilitaryEntity',
        'getArmyList', 'getRegimentList', 'getUnitList', 'getUnitNumberList'
    ]

    def __init__(self, obj, jsonrpc, token, method_name):
        if method_name.startswith("_"):
            raise AttributeError("invalid attribute '%s'" % method_name)

        self.__obj         = obj
        self.__token       = token
        self.__jsonrpc     = jsonrpc
        self.__method_name = method_name

    def __call__(self, *args, **kwargs):
        result = None
        kwargs = dict(kwargs, token = self.__token)

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
        self.bufferIdx = 0

    def __iter__(self):
         return self

    def fetch_next(self):
        self.kwargs['limit'] = self.buffer_size
        self.kwargs['skip'] = self.i
        results = self.method(self.kwargs)
        self.length = results['total']
        self.buffer = results['data']

    def __len__(self):
        if self.length == None:
            self.fetch_next()
        return self.length

    def __next__(self):
        if self.length == None:
            self.fetch_next()

        if self.i >= self.length:
            raise StopIteration()

        self.i += 1
        self.bufferIdx += 1

        if (self.bufferIdx >= len(self.buffer)):
            self.bufferIdx = 0
            self.fetch_next()

        return self.buffer[self.bufferIdx]

    def set_buffer_size(self, buffer_size):
        self.buffer_size = buffer_size

    def set_length(self, length):
        """For testing/debugging purposes
        """
        self.length = length