from pythonmodules.config import Config
import importlib
import unidecode
import re


def normalize(txt):
    return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", '', unidecode.unidecode(txt).lower()))


class NERException(Exception):
    pass


# abstract base class
class NER:
    def tokenize(self, text, language=None):
        raise NotImplemented()

    def tag_entities(self, text, *args, **kwargs):
        raise NotImplemented()


class NERFactory:
    def __init__(self, config = None):
        self.config = Config(config, 'ner')

    def get(self, class_name=None, *args, **kwargs):
        if class_name is None and 'class_name' in self.config:
            class_name = self.config['class_name']
            if 'args' in self.config:
                args = self.config['args']
        if class_name is None:
            class_name = 'StanfordNER'

        m = importlib.import_module(__name__ + '.' + class_name.lower())
        c = getattr(m, class_name)
        return c(*args, **kwargs)
