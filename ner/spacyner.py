import spacy
from . import NER, short2long
import logging

logger = logging.getLogger(__name__)


class SpacyNER(NER):
    def __init__(self, language='xx'):
        self.nlp = spacy.load(language)

    @staticmethod
    def short2long(tag):
        if tag in short2long:
            tag = short2long[tag]
        if tag == '':
            return 'O'

        if tag in NER.ignored_tags:
            return 'O'
        if tag not in NER.allowed_tags:
            logger.warning('Unknown tag "%s"' % tag)
            return 'O'
        return tag

    def tag(self, text, *args, **kwargs):
        return ((word.text, self.short2long(word.ent_type_)) for word in self.nlp(text))


