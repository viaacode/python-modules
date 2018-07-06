from nltk.tag import StanfordNERTagger
from nltk.tokenize import word_tokenize
from pythonmodules.ner import NER
import os


class StanfordNER(NER):
    def __init__(self, path=None):
        if path is None:
            path = os.path.dirname(os.path.realpath(__file__)) + '/'
        self.ner = StanfordNERTagger(path + 'classifiers/english.all.3class.distsim.crf.ser.gz',
                                     path + 'stanford-ner.jar',
                                     encoding='utf-8')

        self.set = set()  # set(['LOCATION', 'ORGANIZATION', 'PERSON'])

    def tag(self, text, group=False, language=None, **kwargs):
        tokenized_text = word_tokenize(text)
        classified_text = self.ner.tag(tokenized_text)
        if len(self.set):
            classified_text = (r for r in classified_text if r[1] in self.set)
        if group:
            classified_text = self.group_tagged_entities(classified_text)
        return classified_text

    @staticmethod
    def group_tagged_entities(tokens, buffer_size=20):
        l = len(tokens)
        idx = 0
        detected = []
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
            detected.append(val)
            idx += 1

        return detected

