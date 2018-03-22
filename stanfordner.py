import nltk
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
import os

class StanfordNER:
    def __init__(self, path = None):
        if path == None:
            path = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.ner = StanfordNERTagger(path + 'classifiers/english.all.3class.distsim.crf.ser.gz',
                                path + 'stanford-ner.jar',
                                encoding='utf-8')

    def tokenize(self, text):
        tokenized_text = word_tokenize(text)
        classified_text = self.ner.tag(tokenized_text)
        return classified_text
