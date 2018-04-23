import nltk
from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
import os

class StanfordNER:
    def __init__(self, path = None, buffer_size = None):
        if path == None:
            path = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.ner = StanfordNERTagger(path + 'classifiers/english.all.3class.distsim.crf.ser.gz',
                                path + 'stanford-ner.jar',
                                encoding='utf-8')
        self.set = set(['LOCATION', 'ORGANIZATION', 'PERSON'])
        self.buffer_size = 20 if buffer_size is None else buffer_size

    def tokenize(self, text):
        tokenized_text = word_tokenize(text)
        classified_text = self.ner.tag(tokenized_text)
        return classified_text

    def tag_entities(self, text):
        types = self.set
        tokens = self.tokenize(text)
        buffer_size = self.buffer_size
        l = len(tokens)
        idx = 0
        detected = []
        while idx < l:
            token = tokens[idx]
            if not token[1] in types:
                idx += 1
                continue
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
            detected.append(val)
            idx += 1

        return detected
