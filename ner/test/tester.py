from pythonmodules.ner.gmbner import read_gmb
from pythonmodules.ner import NER, NERFactory, simplify_bio_tags
from itertools import chain
from collections import namedtuple
import logging
from tqdm import tqdm
from pythonmodules.profiling import timeit

logger = logging.getLogger(__name__)


class Samples:
    def __init__(self):
        self.data = None

    def init(self):
        if self.data is not None:
            return
        reader = read_gmb()
        data = list(reader)
        self.data = (data[:int(len(data) * 0.1)], data[int(len(data) * 0.1):])

    def training(self):
        if self.data is None:
            self.init()
        return self.data[1]

    def test(self, amount=500):
        # optim: no need to read all the data if we only using test
        if self.data is None:
            reader = read_gmb()
            return (next(reader) for i in range(amount))
        return self.data[0][:amount]

    def test_names(self, amount=500):
        if amount is None:
            self.init()
            data = chain(*self.data)
        else:
            data = self.test(amount)

        Phrase = namedtuple('Phrase', ['text', 'tags'])

        for line in data:
            yield Phrase(' '. join([d[0][0] for d in line]),
                         list(simplify_bio_tags([(d[0][0], d[0][1], d[1]) for d in line])))


class Tester:
    def __init__(self, taggers=None):
        if taggers is None:
            taggers = NERFactory().get()

        if isinstance(taggers, (str, NER)):
            taggers = [taggers]

        self.taggers = [NERFactory().get(tagger) if type(tagger) is str else tagger for tagger in taggers]
        logger.debug(str(self.taggers))
        self.sampler = Samples()

    @staticmethod
    def filter_tags(tags):
        return [s for s in tags if len(s[0]) != 1 or s[0] not in '\'". ,;!%']

    def test(self, amount=None):
        samples = self.sampler.test_names(amount)
        if amount is None:
            samples = list(samples)
            amount = len(samples)
        totals = [dict(same=0, time=0) for i in range(len(self.taggers))]
        total_tags = 0
        progress = tqdm(total=amount*len(self.taggers))
        timer = timeit()
        for sample in samples:
            orig_tags = [tag[1] for tag in Tester.filter_tags(sample.tags)]
            ntags = len(orig_tags)
            total_tags += ntags
            for n, tagger in enumerate(self.taggers):
                progress.update()
                cls = type(tagger).__name__
                timer.restart()
                tags = Tester.filter_tags(tagger.tag(sample.text))
                elapsed = timer.elapsed()
                if len(tags) != ntags:
                    logger.warning("Samples are of different size for %s (%d vs %d): %s" % (cls, len(tags), ntags, str((tags, orig_tags))))
                    continue
                tags = [tag[1] for tag in tags]
                sames = sum([tags[i] == orig_tags[i] for i in range(ntags)])
                # pct = sames / ntags * 100
                # logger.debug('%s: %d%%' % (cls, pct))
                totals[n]['same'] += sames
                totals[n]['time'] += elapsed

        Stats = namedtuple('Stats', ['accuracy', 'time'])
        return [Stats(t['same'] / total_tags * 100, t['time']) for t in totals]

