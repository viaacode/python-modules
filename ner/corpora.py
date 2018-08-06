import os
from collections import namedtuple
import logging
from . import bio_to_entity_name

logger = logging.getLogger(__name__)


EntityResults = namedtuple('EntityResults', ('phrase', 'entities'))
Entity = namedtuple('Entity', ('text', 'entity'))


class Corpus:
    def read_entities(self):
        """
        :return: Iterable
        """
        raise NotImplementedError("Needs to be implemented")


class Europeana:
    """
    After analysis: yikes, Europeana manual corpus annotation is quite poor...
    DON'T use it for evaluation!
    """
    corpora = {
        'nl':  ('enp_NL.kb.bio/enp_NL.kb.bio',),
        'de': ('enp_DE.lft.bio/enp_DE.lft.bio', 'enp_DE.onb.bio/enp_DE.onb.bio'),
        'fr': ('enp_FR.bnf.bio/enp_FR.bnf.bio', )
    }

    phrase_separators = '.?!'

    def __init__(self, path=None, languages=None):
        if path is None:
            path = os.path.dirname(os.path.realpath(__file__)) + '/europeana/'
        self.path = path

        if languages is None:
            languages = self.corpora.keys()
        elif type(languages) is str:
            languages = [languages]

        self.languages = languages

    def read_entities(self):
        for language in self.languages:
            for corpus in self.corpora[language]:
                with open(self.path + corpus) as file:
                    entities = []
                    for line in file.readlines():
                        if line[:4] == '<-- ':
                            continue
                        line = line.rstrip('\n\r')
                        if line == '? O':
                            phrase = ' '.join([entity.text for entity in entities])
                            yield EntityResults(phrase=phrase, entities=entities)
                            entities = []
                        else:
                            items = line.split(' ')
                            if len(items) < 2:
                                items.append('O')
                            elif len(items) > 2:
                                logger.warning('Invalid format "%s"', line)
                                continue
                            items[1] = bio_to_entity_name(items[1])
                            entity = Entity(*items)
                            entities.append(entity)
                    if len(entities):
                        phrase = ' '.join([entity.text for entity in entities])
                        yield EntityResults(phrase=phrase, entities=entities)


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
                entities=[Entity(text=word[0][0], entity=bio_to_entity_name(word[1])) for word in phrase]
            )
            yield entity_results

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
