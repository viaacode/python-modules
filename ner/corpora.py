import os
from collections import namedtuple
import logging
from . import NER, short2long

logger = logging.getLogger(__name__)


class Corpus:
    def read_entities(self):
        """
        :return: Iterable
        """
        raise NotImplementedError("Needs to be implemented")


class Europeana:
    def __init__(self, path=None):
        if path is None:
            path = os.path.dirname(os.path.realpath(__file__)) + '/europeana/'
        self.path = path
        # TODO


EntityResults = namedtuple('Bio', ('phrase', 'entities'))
Entity = namedtuple('Entity', ('text', 'entity'))


# based on https://nlpforhackers.io/named-entity-extraction/
class GMB(Corpus):
    def __init__(self, path=None):
        if path is None:
            path = os.path.dirname(os.path.realpath(__file__)) + '/gmb/'
        self.path = path

    def read_entities(self):
        for phrase in self.read_nltk():
            entity_results = EntityResults(
                phrase=' '.join([word[0][0] for word in phrase]),
                entities=[Entity(text=word[0][0], entity=self.simplify_bio_tag(word[1])) for word in phrase]
            )
            yield entity_results

    @staticmethod
    def simplify_bio_tag(bio):
        try:
            bio = short2long[bio[2:].upper()]
            if bio not in NER.allowed_tags and bio not in NER.ignored_tags:
                logger.debug('Unknown NER tag "%s"' % bio)
                bio = 'O'
        except KeyError:
            bio = 'O'
        return bio

    def read_sentence(self):
        for root, dirs, files in os.walk(self.path):
            for filename in files:
                if not filename.endswith(".tags"):
                    continue
                with open(os.path.join(root, filename), 'rb') as file_handle:
                    for annotated_sentence in file_handle.read().decode('utf-8').strip().split('\n\n'):
                        yield annotated_sentence

    def read_nltk(self):
        for annotated_sentence in self.read_sentence():
            annotated_tokens = [seq for seq in annotated_sentence.split('\n') if seq]
            standard_form_tokens = []
            for annotated_token in annotated_tokens:
                standard_form_tokens.append(self.token_to_nltk_compatible_tuple(annotated_token))

            conll_tokens = GMB.to_conll_iob(list(standard_form_tokens))

            # Make it NLTK Classifier compatible - [(w1, t1, iob1), ...] to [((w1, t1), iob1), ...]
            # Because the classfier expects a tuple as input, first item input, second the class
            yield [((w, t), iob) for w, t, iob in conll_tokens]

    @staticmethod
    def token_to_nltk_compatible_tuple(annotated_token):
        annotations = annotated_token.split('\t')
        word, tag, ner = annotations[0], annotations[1], annotations[3]

        if ner != 'O':
            ner = ner.split('-', 2)[0]

        if tag in ('LQU', 'RQU'):  # Make it NLTK compatible
            tag = "``"

        return word, tag, ner

    @staticmethod
    def to_conll_iob(annotated_sentence):
        """
        `annotated_sentence` = list of triplets [(w1, t1, iob1), ...]
        Transform a pseudo-IOB notation: O, PERSON, PERSON, O, O, LOCATION, O
        to proper IOB notation: O, B-PERSON, I-PERSON, O, O, B-LOCATION, O
        """
        proper_iob_tokens = []
        for idx, annotated_token in enumerate(annotated_sentence):
            tag, word, ner = annotated_token

            if ner != 'O':
                if idx == 0:
                    ner = "B-" + ner
                elif annotated_sentence[idx - 1][2] == ner:
                    ner = "I-" + ner
                else:
                    ner = "B-" + ner
            proper_iob_tokens.append((tag, word, ner))
        return proper_iob_tokens
