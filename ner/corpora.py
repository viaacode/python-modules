import os


class Corpus:
    def get_bio(self):
        """
        :return: Iterable
        """
        raise NotImplementedError("Needs to be implemented")


# based on https://nlpforhackers.io/named-entity-extraction/
class GMB(Corpus):
    def __init__(self, path=None):
        if path is None:
            path = os.path.dirname(os.path.realpath(__file__)) + '/gmb/'
        self.path = path

    def get_bio(self):
        raise NotImplementedError('todo')

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

    def files(self):
        return ((root, filename)
                for root, dirs, files in os.walk(self.path)
                for filename in files if filename.endswith(".tags"))

    def read_gmb(self):
        for root, filename in self.files():
            with open(os.path.join(root, filename), 'rb') as file_handle:
                for annotated_sentence in file_handle.read().decode('utf-8').strip().split('\n\n'):
                    annotated_tokens = [seq for seq in annotated_sentence.split('\n') if seq]
                    standard_form_tokens = []

                    for idx, annotated_token in enumerate(annotated_tokens):
                        annotations = annotated_token.split('\t')
                        word, tag, ner = annotations[0], annotations[1], annotations[3]

                        if ner != 'O':
                            ner = ner.split('-')[0]

                        if tag in ('LQU', 'RQU'):  # Make it NLTK compatible
                            tag = "``"

                        standard_form_tokens.append((word, tag, ner))

                    conll_tokens = GMB.to_conll_iob(standard_form_tokens)

                    # Make it NLTK Classifier compatible - [(w1, t1, iob1), ...] to [((w1, t1), iob1), ...]
                    # Because the classfier expects a tuple as input, first item input, second the class
                    yield [((w, t), iob) for w, t, iob in conll_tokens]
