from pythonmodules.ner.corpora import GMB
from pythonmodules.ner import NER, NERFactory
from collections import namedtuple, defaultdict
import logging
from tqdm import tqdm
from pythonmodules.profiling import timeit
from pycm import ConfusionMatrix

logger = logging.getLogger(__name__)


class Samples:
    def __init__(self, corpus, test_pct=None):
        self.data = None
        self.corpus = corpus
        self.test_pct = test_pct if test_pct is not None else 0.1

    def init(self):
        if self.data is not None:
            return
        reader = self.corpus.read_nltk()
        data = list(reader)
        splitpoint = int(len(data) * self.test_pct)
        self.data = (data[:splitpoint], data[splitpoint:])

    def training(self):
        if self.data is None:
            self.init()
        return self.data[1]

    def test(self, amount=500):
        # optim: no need to read all the data if we only using test
        if self.data is None:
            reader = self.corpus.read_nltk()
            return (next(reader) for i in range(amount))
        return self.data[0][:amount]


class Tester:
    def __init__(self, taggers=None):
        if taggers is None:
            taggers = NERFactory().get()

        if isinstance(taggers, (str, NER)):
            taggers = [taggers]

        self.taggers = [NERFactory().get(tagger) if type(tagger) is str else tagger for tagger in taggers]
        self.corpus = GMB()

    @staticmethod
    def filter_tags(tags):
        """
        Filters out some unimportant tags
        :param tags:
        :return:
        """
        return list(NER.filter_tags([s for s in tags if s[0] != '``' and (len(s[0]) != 1 or s[0] not in '\'". ,;!%')]))

    @staticmethod
    def fix_tags_counts(tags, correct_tags):
        """
        Makes sure (in an extremely naive way) that tags count is in line with correct_tags (mosly related to different
        tokenizer rules)
        :param tags:
        :param correct_tags:
        :return:
        """
        fixed = []
        ltags = len(tags)
        n = 0
        for i in range(len(correct_tags)):
            name = ''
            while n < ltags and name != correct_tags[i][0]:
                # print('%s %d - %d' % (name, i, n))
                # print(str(tags[n]))
                name += tags[n][0]
                # print(name + ' vs. ' + correct_tags[i][0])
                n += 1
            fixed.append((name, tags[n-1][1]))

        return fixed

    def test(self, amount=500):
        # known_tags = NER.allowed_tags
        samples = self.corpus.read_entities()

        if amount == 0:
            logger.info("Loading the entire corpus in memory, this may take a while...")
            # preload all samples to have an amount available for progress indicator
            samples = list(samples)
            amount = len(samples)

        totals = [defaultdict(lambda: 0) for i in range(len(self.taggers))]
        # stats = [defaultdict(lambda: defaultdict(lambda: 0)) for i in range(len(self.taggers))]

        total_tags = 0
        progress = tqdm(total=amount*len(self.taggers))
        timer = timeit()
        full_orig_tags = []
        full_predict_tags = [[] for i in range(len(self.taggers))]

        for sample_index, sample in enumerate(samples):
            if amount and sample_index >= amount:
                break
            sample_tags = Tester.filter_tags(sample.entities)
            orig_tags = [tag[1] for tag in sample_tags]
            ntags = len(orig_tags)
            total_tags += ntags
            full_orig_tags.extend(orig_tags)
            for n, tagger in enumerate(self.taggers):
                progress.update()
                cls = type(tagger).__name__
                timer.restart()
                tags = Tester.filter_tags(tagger.tag(sample.phrase))
                elapsed = timer.elapsed()

                # check and do a naive attempt to fix different tag lengths
                if len(tags) != ntags:
                    tags = Tester.fix_tags_counts(tags, sample_tags)
                    if len(tags) != ntags:
                        logger.error("Samples are of different size for %s (%d vs %d): \nTAGGER:   %s\nORIGINAL: %s",
                                     cls, len(tags), ntags, tags, sample_tags)
                        continue

                tag_types = [tag[1] for tag in tags]
                zipped = list(zip(tag_types, orig_tags))
                sames = sum([tag[0] == tag[1] for tag in zipped])

                # for tags in zipped:
                #     if tags[0] in known_tags:
                #         if tags[0] == tags[1]:
                #             stats[n][tags[0]]['tp'] += 1
                #         else:
                #             stats[n][tags[0]]['fp'] += 1
                #     elif tags[1] in known_tags:
                #         stats[n][tags[1]]['fn'] += 1
                #
                #     for tag in known_tags:
                #         if tag != tags[1]:
                #             stats[n][tag]['tn'] += 1

                totals[n]['same'] += sames
                totals[n]['time'] += elapsed
                full_predict_tags[n].extend(tag_types)

        Stats = namedtuple('Stats', ['accuracy', 'time', 'total_checked', 'confusion_matrix'])
        return [Stats(
                    t['same'] / total_tags * 100,
                    t['time'],
                    # stats[i],
                    total_tags,
                    ConfusionMatrix(actual_vector=full_orig_tags, predict_vector=full_predict_tags[i])
                ) for i, t in enumerate(totals)]

