from jsonrpc_requests import Server, ProtocolError
from .config import Config

import logging
import http.client as http_client
from urllib.parse import urlparse
import datetime
from .decorators import retry
from .ner import normalize

from collections import namedtuple, defaultdict
from collections.abc import Mapping


logger = logging.getLogger(__name__)
retry = retry(5, logger=logger, sleep=1)


Names = namedtuple('Names', ['name', 'name_normalized', 'firstnames', 'lastnames', 'firstnames_normalized',
                             'lastnames_normalized', 'initials', 'variations', 'variations_normalized'])

Person = namedtuple('Person', ['id', 'names', 'died_age', 'died_date', 'born_date', 'born_place',
                               'died_place', 'events', 'gender', 'victim_type',
                               'victim_type_details', 'relations', 'description', 'summary'])


class Namenlijst:
    __jsonrpc = None
    __config = None
    __token = None

    def __init__(self, config=None, log_http_requests=None):
        self.__config = Config(config, 'namenlijst')
        self.timeout = None
        kwargs = dict()
        if type(log_http_requests) == bool:
            self.set_log_http_requests(log_http_requests)

        if not self.__config.is_false('timeout'):
            kwargs['timeout'] = int(self.__config['timeout'])

        url = urlparse(self.__config['api_host'])
        if not self.__config.is_false('proxy'):
            kwargs['proxies'] = {url.scheme: self.__config['proxy']}

        self._user = url.username
        self._passwd = url.password
        url = url._replace(netloc="{}:{}".format(url.hostname, url.port)).geturl()
        self.__jsonrpc = Server(url, **kwargs)

    # def findPersonAdvanced(query = None):
    # todo

    @retry
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
    
    def get_person_full(self, nmlid: str, language: str=None) -> Person:
        """
        Get all info about an nmlid, adds events, etc.

        :param nmlid:
        :param language:
        :return: Person
        """

        person = self.findPerson(document={"_id": nmlid},
                                 options=["EXTEND_BORN_PLACE", "EXTEND_DIED_PLACE", "EXTEND_DOCUMENTS"],
                                 limit=1)
        if not len(person):
            raise KeyError('Person with id "%s" not found', nmlid)

        person = next(person)
        person['names'] = Conversions.get_names(person)

        died = Conversions.sort_date_to_date(person['sort_died_date'])
        born = Conversions.sort_date_to_date(person['sort_born_date'])
        # fix died_age if missing
        if not person['died_age']:
            if born is None or died is None:
                person['died_age'] = None
            else:
                age = died.year - born.year
                if died.month < born.month or (died.month == born.month and died.day < born.day):
                    age -= 1
                person['died_age'] = age

        # add events
        person['events'] = self.findEvent(document={"person_id": nmlid}, options=['EXTEND_PLACE'])
        person['military'] = list(self.findMilitaryEntity(document={"person_id": nmlid}))

        died_age = str(person['died_age']) if person['died_age'] else '?'
        died_date = died if died else '?'

        summary = '%s: died at age %s (%s)' % (person['names'].name, died_age, died_date)

        # only pass relevant data
        return Person(
            id=nmlid,
            names=person['names'],
            died_age=person['died_age'],
            died_date=died,
            born_date=born,
            events=Conversions.convert_events(person['events'], language),
            born_place=Conversions.convert_place(person['extend_born_place'], language),
            died_place=Conversions.convert_place(person['extend_died_place'], language),
            gender=person['gender'],
            victim_type=person['victim_type'],
            victim_type_details=person['victim_type_details'],
            relations=person['relations'],
            description=person['description'],
            summary=summary
        )


class Conversions:
    @staticmethod
    def normalize(txt):
        return normalize(txt).strip()

    @staticmethod
    def normalize_no_num(txt):
        return normalize(txt, regex="[^a-z ]").strip()

    @classmethod
    def convert_events(cls, events, language=None):
        result = defaultdict(None)
        for row in events:
            row = cls.convert_dates(row, ['start', 'end'])
            row['place'] = None
            if 'extend_place' in row:
                if len(row['extend_place']):
                    row['place'] = cls.convert_place(row['extend_place'], language)
                # del row['extend_place']
                row = {k: row[k] for k in row if k[:6] != 'place_'}
            # del row['person_id']  # superfluous
            result[row['type']] = row
        return result

    @staticmethod
    def get_names(row, filter_=None, normalize=True):
        if normalize is True:
            normalize = Conversions.normalize
        name = '%s %s' % (row['surname'], row['familyname'])

        firstnames = [row['surname']]
        if 'alternative_surnames' in row:
            firstnames.extend(row['alternative_surnames'])

        lastnames = [row['familyname']]

        if 'alternative_familynames' in row:
            lastnames.extend(row['alternative_familynames'])

        if filter_ is not None:
            firstnames = filter(filter_, firstnames)
            lastnames = filter(filter_, lastnames)

        firstnames = set(firstnames)
        lastnames = set(lastnames)

        variations = list('%s %s' % (fname, lname) for fname in firstnames for lname in lastnames)
        variations.extend('%s %s' % (lname, fname) for fname in firstnames for lname in lastnames)
        variations = set(variations)

        if normalize is not None and normalize is not False:
            firstnames_normalized = set(name for name in map(normalize, firstnames))
            lastnames_normalized = set(name for name in map(normalize, lastnames))
            name_normalized = normalize(name)
            variations_normalized = set(map(normalize, variations))
        else:
            firstnames_normalized = firstnames
            lastnames_normalized = lastnames
            name_normalized = name
            variations_normalized = variations

        return Names(
            name=name,
            name_normalized=name_normalized,
            firstnames=firstnames,
            lastnames=lastnames,
            firstnames_normalized=firstnames_normalized,
            lastnames_normalized=lastnames_normalized,
            initials=row['initials'],
            variations=variations,
            variations_normalized=variations_normalized
        )

    @classmethod
    def convert_dates(cls, row, keys):
        if type(keys) is str:
            keys = [keys]
        for key in keys:
            # if row[key] is not None:
            row[key] = cls.get_date(row, key)
            # if ('sort_%s_date' % key) in row:
            #     del row['sort_%s_date' % key]
        return row

    @staticmethod
    def get_date(row, key):
        values = []
        keys = [(k % key) for k in ('%s_year', '%s_month', '%s_day')]
        for k in keys:
            if k not in row:
                raise KeyError("Unknown key '%s'" % k)
            values.append(row[k])
            del row[k]

        if any(type(value) is not int for value in values):
            return None

        # I know below "fixes" aren't really clean, but hey, we work with the data we have...
        if values[1] > 12:
            logger.warning('%s %s:%s: Month > 12, assume month and date are swapped for %s',
                           row['person_id'], row['type'], key, '/'.join(map(str, values)))
            values = [values[0], values[2], values[1]]

        try:
            return datetime.date(*values)
        except ValueError as e:
            if str(e) == 'day is out of range for month':
                logger.warning('%s for %s: will change day to 28', str(e), '/'.join(map(str, values)))
                values[2] = 28
            else:
                return None
            return datetime.date(*values)

    @staticmethod
    def sort_date_to_date(date: str):
        if len(date) < 8:
            return None
        year, month, day = date[:4], date[4:6], date[6:]
        try:
            year = int(year)
        except ValueError:
            return None

        try:
            month = int(month)
        except ValueError:
            month = 6

        try:
            day = int(day)
        except ValueError:
            day = 1

        try:
            return datetime.date(year, month, day)
        except ValueError as e:
            if str(e) == 'day is out of range for month':
                logger.warning('%s for %s: will change day to 28', str(e), date)
                day = 28
            return datetime.date(year, month, day)

    @staticmethod
    def convert_place(data, language=None):
        return LanguageData(data, language)


LanguageDataNone = defaultdict(None)


class LanguageData(Mapping):
    def __init__(self, data, language=None):
        if 'languages' not in data:
            raise KeyError('Missing languages')
        self.__language = None
        self.__data = data
        if language is None:
            language = 'en'
        self.language = language

    def __dict__(self):
        return {k: self[k] for k in self.keys()}

    def __repr__(self):
        return '<%s:%s(%s)>' % (type(self).__name__, self.__language, str(self.__dict__()))

    @property
    def language(self):
        return self.__language

    @language.setter
    def language(self, language):
        if language not in self.__data['languages']:
            raise KeyError("Language '%s' not found", language)
        self.__language = language

    def __getitem__(self, k):
        if k in self.__data['languages'][self.language]:
            return self.__data['languages'][self.language][k]
        return self.__data[k]

    def __len__(self) -> int:
        return len(self.keys())

    def keys(self):
        keys = list(self.__data.keys())
        keys.extend(self.__data['languages'][self.language].keys())
        keys = set(keys)
        return (key for key in keys if key != 'languages')

    def _asdict(self) -> dict:
        d = {k: self[k] for k in self.keys()}
        d['__language'] = self.language
        return d

    def __contains__(self, item):
        return item in self.keys()

    def __iter__(self):
        return iter(self.keys())


class Method:
    __iterable_methods = [
        'findPerson', 'findEvent', 'findPlace', 'findMemorial', 'findMilitaryEntity',
        'getArmyList', 'getRegimentList', 'getUnitList', 'getUnitNumberList'
    ]
    
    def __init__(self, obj, jsonrpc, token, method_name):
        logger.debug('%s %s', __name__, method_name)

        if method_name.startswith("_"):
            raise AttributeError("invalid attribute '%s'" % method_name)

        self.__obj = obj
        self.__token = token
        self.__jsonrpc = jsonrpc
        self.__method_name = method_name

    @retry
    def _call_iterable(self, *args, **kwargs):
        kwargs['total'] = 'true'
        result = ResultIterator(getattr(self.__jsonrpc, self.__method_name), kwargs)
        return result

    @retry
    def _call_non_iterable(self, *args, **kwargs):
        attempts = 2
        while attempts > 0:
            try:
                attempts -= 1
                return getattr(self.__jsonrpc, self.__method_name)(kwargs)
            except ProtocolError as e:
                if attempts == 0:
                    raise e
                logger.warning("Call to %s yieled exception '%s', refreshing token and trying again",
                               self.__method_name, e)

                kwargs['token'] = self.__obj.refresh_token()
                self.__token = kwargs['token']

    def __call__(self, *args, **kwargs):
        logger.debug("NMLD call %s with args(%s, %s)", self.__method_name, args, kwargs)
        kwargs = dict(kwargs, token=self.__token)

        if self.__method_name in self.__iterable_methods:
            return self._call_iterable(*args, **kwargs)

        return self._call_non_iterable(*args, **kwargs)


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
        results = self._get_next_results()
        self.length = results['total']
        self.buffer = results['data']

    @retry
    def _get_next_results(self):
        return self.method(self.kwargs)

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
