from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
from pythonmodules.ner import NER
import os


class StanfordNER(NER):
    def __init__(self, path=None):
        if path is None:
            path = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.ner = StanfordNERTagger(path + 'stanford/classifiers/english.all.3class.distsim.crf.ser.gz',
                                     path + 'stanford/stanford-ner.jar',
                                     encoding='utf-8')

        self.set = NER.allowed_tags

    def tag(self, text, language=None, **kwargs):
        tokenized_text = word_tokenize(text)
        classified_text = self.ner.tag(tokenized_text)
        return classified_text

