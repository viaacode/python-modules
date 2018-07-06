from pythonmodules.config import Config
import importlib
import unidecode
import re
import json
import logging

logger = logging.getLogger(__name__)


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
    KNOWN_TAGGERS = ('StanfordNER', 'GMBNER')

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
