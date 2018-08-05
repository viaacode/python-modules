from pythonmodules.config import Config
import importlib
import unidecode
import re
import json
import logging


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

    ignored_tags = (ORGANISATION, 'ORGANIZATION')

    allowed_tags = (PERSON,
                    # ORGANISATION,
                    LOCATION)

    def tag(self, text, *args, **kwargs):
        raise NotImplemented()

    @staticmethod
    def filter_tags(tags):
        return ((tag[0], tag[1] if tag[1] in NER.allowed_tags else 'O') for tag in tags)

    @staticmethod
    def group_tagged_entities(tokens, buffer_size=20):
        l = len(tokens)
        idx = 0
        while idx < l:
            token = tokens[idx]
            text = token[0]
            start_idx = idx
            while idx + 1 < l:
                idx += 1
                if tokens[idx][1] != token[1]:
                    break
                text += ' ' + tokens[idx][0]
            val = {
                'type': token[1],
                'value': text
            }
            if buffer_size:
                val['context'] = ' '.join([t[0] for t in tokens[start_idx - buffer_size:idx + buffer_size]])
            yield val


class NERFactory:
    KNOWN_TAGGERS = ('StanfordNER', 'TrainedNER', 'StanfordNERClient')

    def __init__(self, config=None):
        self.config = Config(config, 'ner')

    def get(self, class_name=None, *args, **kwargs):
        if class_name is None and 'class_name' in self.config:
            class_name = self.config['class_name']
            if class_name not in NERFactory.KNOWN_TAGGERS:
                logger.info("Class '%s' is not known to NERFactory", class_name)
            if 'args' in self.config:
                print(self.config['args'])
                args = json.loads(self.config['args'])
        if class_name is None:
            class_name = NERFactory.KNOWN_TAGGERS[0]

        m = importlib.import_module(__name__ + '.' + class_name.lower())
        c = getattr(m, class_name)
        return c(*args, **kwargs)


