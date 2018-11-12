from pythonmodules.config import Config
import importlib
from unidecode import unidecode
import re
import json
import logging


logger = logging.getLogger(__name__)
short2long = dict(
    LOC='LOCATION',
    ORG='ORGANIZATION',
    PER='PERSON',
    GEO='LOCATION',
    TIM='TIME',
    ART='ARTIFACT',
    GPE='GEOPOLITICALENTITY',
    EVE='EVENT',
    NAT='NATURALEVENT',
    MISC='MISCELLANEOUS',
    DATE='DATE',
    TIME='Times smaller than a day',
    PERCENT='PERCENT',
    CARDINAL='CARDINAL',
    ORDINAL='ORDINAL',
    MONEY='MONEY',
    LANGUAGE='LANGUAGE',
    LAW='LAW',
    WORK_OF_ART='WORKOFART',


    # Nationalities or religious or political groups
    NORP='NATIONALITY',
    # Buildings, airports, highways, bridges, etc.
    FAC='FACILITY',
    FACILITY='FACILITY',
    # Objects, vehicles, foods, etc. (not services)
    PRODUCT='PRODUCT',
    # Named hurricanes, battles, wars, sports events, etc.
    EVENT='EVENT',
    # Measurements, as of weight or distance
    QUANTITY='QUANTITY',

)


def bio_to_entity_name(bio_tag):
    if bio_tag == 'O' or bio_tag == '':
        return 'O'

    try:
        bio = short2long[bio_tag[2:].upper()]
        if bio in NER.ignored_tags:
            return 'O'
        if bio not in NER.allowed_tags:
            raise KeyError(bio)
        return bio
    except KeyError:
        logger.warning('Unknown NER tag "%s"' % bio_tag)
        return 'O'


def normalize(txt, regex="[^a-z0-9 ]"):
    return re.sub(r"\s+", " ", re.sub(regex, '', unidecode(txt).lower()))


class NERException(Exception):
    pass


# abstract base class
class NER:
    PERSON = 'PERSON'
    ORGANISATION = 'ORGANISATION'
    LOCATION = 'LOCATION'

    allowed_tags = (PERSON,
                    # ORGANISATION,
                    LOCATION)

    def tag(self, text, *args, **kwargs):
        raise NotImplemented()

    @staticmethod
    def filter_tags(tags):
        for tag in tags:
            if tag[1] not in NER.allowed_tags and tag[1] not in NER.ignored_tags and tag[1] != 'O':
                logger.warning('unknown tage "%s"' % tag[1])
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


NER.ignored_tags = [tag for tag in short2long.values() if tag not in NER.allowed_tags]


class NERFactory:
    KNOWN_TAGGERS = (
        'StanfordNER',
        'TrainedNER',
        'StanfordNERClient',
        'SpacyNER'
    )
    
    def __init__(self, config=None):
        self.config = Config(config, 'ner')

    def get(self, class_name=None, *args, **kwargs):
        if class_name is None and 'class_name' in self.config:
            class_name = self.config['class_name']
            if class_name not in NERFactory.KNOWN_TAGGERS:
                logger.info("Class '%s' is not known to NERFactory", class_name)
            if 'args' in self.config:
                logger.debug('NERFactory args:')
                logger.debug(self.config['args'])
                args = json.loads(self.config['args'])
        if class_name is None:
            class_name = NERFactory.KNOWN_TAGGERS[0]

        m = importlib.import_module(__name__ + '.' + class_name.lower())
        c = getattr(m, class_name)
        return c(*args, **kwargs)


