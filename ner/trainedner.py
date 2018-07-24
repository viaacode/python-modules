from pythonmodules.ner import NER, simplify_bio_tags
import pickle
from nltk import pos_tag, word_tokenize


class TrainedNER(NER):
    def __init__(self, file='gmb-2.2.0.pickle'):
        if file is None:
            raise FileNotFoundError('A pickled chunker should be passed')
        self.chunker = pickle.load(open(file, 'rb'))

    def tag(self, text, language=None, **kwargs):
        tags = self.chunker.parse(pos_tag(word_tokenize(text)))
        return simplify_bio_tags(tags)

