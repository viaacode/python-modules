from pythonmodules.ner import NER, bio_to_entity_name
import pickle
from nltk import pos_tag, word_tokenize
import logging

logger = logging.getLogger(__name__)


class TrainedNER(NER):
    def __init__(self, file='gmb-2.2.0.pickle'):
        if file is None:
            raise FileNotFoundError('A pickled chunker should be passed')
        self.chunker = pickle.load(open(file, 'rb'))

    def tag(self, text, language=None, **kwargs):
        tags = self.chunker.parse(pos_tag(word_tokenize(text)))
        return ((word, bio_to_entity_name(bio)) for word, pos, bio in tags)

