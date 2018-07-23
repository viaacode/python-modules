from pythonmodules.config import Config
import importlib
import unidecode
import re
import json
import logging
from nltk.sem.relextract import short2long


logger = logging.getLogger(__name__)
short2long = dict(LOC='LOCATION', ORG='ORGANIZATION', PER='PERSON', GEO='LOCATION')

def normalize(txt):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", '', unidecode.unidecode(txt).lower()))


class NERException(Exception):
    pass


# abstract base class
class NER:
    PERSON = 'PERSON'
    ORGANISATION = 'ORGANISATION'
    LOCATION = 'LOCATION'

    allowed_tags = (PERSON, ORGANISATION, LOCATION)

    def tag(self, text, *args, **kwargs):
        raise NotImplemented()


class NERFactory:
    KNOWN_TAGGERS = ('StanfordNER', 'GMBNER', 'StanfordNERClient')

    def __init__(self, config=None):
        self.config = Config(config, 'ner')

    def get(self, class_name=None, *args, **kwargs):
        if class_name is None and 'class_name' in self.config:
            class_name = self.config['class_name']
            if class_name not in NERFactory.KNOWN_TAGGERS:
                logger.info("Class '%s' is not known to NERFactory" % class_name)
            if 'args' in self.config:
                print(self.config['args'])
                args = json.loads(self.config['args'])
        if class_name is None:
            class_name = NERFactory.KNOWN_TAGGERS[0]

        m = importlib.import_module(__name__ + '.' + class_name.lower())
        c = getattr(m, class_name)
        return c(*args, **kwargs)


def simplify_bio_tags(tags):
    for word, pos, bio in tags:
        try:
            bio = short2long[bio[2:].upper()]
            if bio not in NER.allowed_tags:
                bio = 'O'
        except KeyError:
            bio = 'O'
        yield (word, bio)
