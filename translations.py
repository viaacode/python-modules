import requests
import json
from functools import partial
import logging
import csv
import os
import sys
from .config import Config


logger = logging.getLogger(__name__)

conf = Config(section='translations')
proxy = {}

if not conf.is_false('proxy'):
    proxy = {'http': conf['proxy'], 'https': conf['proxy']}


def translate(from_language, to_language, text):
    if not len(text):
        return []

    url = 'https://%s.wiktionary.org/w/api.php' % from_language
    data = {
        "action": 'query',
        "prop": 'iwlinks',
        "format": 'json',
        "iwlimit": 30,
        "iwprefix": to_language,
        "titles": text
    }

    resp = requests.get(url, params=data, proxies=proxy, timeout=10)

    if resp.status_code != 200:
        return []

    resp = json.loads(resp.content.decode())
    resp = resp['query']['pages']
    if not len(resp):
        return []

    results = []
    for v in resp.values():
        if 'iwlinks' not in v or not v['iwlinks']:
            continue
        for link in v['iwlinks']:
            results.append(link['*'])

    return results


def translator(from_language, to_language):
    return partial(translate, from_language, to_language)


class Translator:
    _instances = {}

    def __init__(self, from_language, to_language):
        self.from_langauge = from_language.lower()
        self.to_language = to_language.lower()
        self._translator = translator(self.from_langauge, self.to_language)
        self._cache = None
        self._cache_file = '/tmp/translations_%s_%s.csv' % (self.from_langauge, self.to_language)

    @classmethod
    def factory(cls, *args):
        key = ','.join(args)
        if key not in cls._instances:
            cls._instances[key] = cls(*args)
        return cls._instances[key]

    def load_cached(self):
        if self._cache is not None:
            return

        if not os.path.exists(self._cache_file):
            self._cache = {}
            return

        with open(self._cache_file, 'r') as f:
            reader = csv.reader(f)
            self._cache = dict(list(reader))

    def translate(self, text):
        if not len(text):
            return []
        
        self.load_cached()
        if text in self._cache:
            return self._cache[text].split('|')

        try:
            translated = self._translator(text)
            self._cache[text] = '|'.join(translated)

            with open(self._cache_file, 'a+') as f:
                writer = csv.writer(f)
                writer.writerow([text, '|'.join(translated)])
            return translated
        except Exception as e:
            logger.error(e)
            return []


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logger.setLevel(logging.DEBUG)
    print(translate(*sys.argv[1:]))
