import re
import unidecode
from collections import namedtuple, defaultdict
from itertools import chain
from pythonmodules.profiling import timeit
from pythonmodules.namenlijst import Namenlijst
from pythonmodules.mediahaven import MediaHaven
import logging


logger = logging.getLogger(__name__)

Score = namedtuple('Score', ['text', 'min_distance', 'match'])
Scores = namedtuple('Scores', ['amount', 'score', 'matches', 'rating'])
Rating = namedtuple('Rating', ['scores', 'total'])


class Matcher:
    def __init__(self, text, base):
        self.text = self.normalize(text)
        if type(base) is str:
            base = [base]
        
        self.initial_matches = list(chain(*[self.find(b) for b in base]))
        self.indices = list(chain(*map(lambda x: x.span(), self.initial_matches)))
        self.indices.sort()
        if not len(self.indices):
            logger.warning('Could not find base text "%s"', base)
            # raise IndexError('Could not find base text "%s"' % base)

    @staticmethod
    def normalize(txt):
        return re.sub(r"\s+", " ", re.sub(r"[^a-z ]", '', unidecode.unidecode(txt).lower()))

    @classmethod
    def search_term(cls, term):
        """
        eg. 'Victor Hugo' -> 'v\\s*i\\s*c\\s*t\\s*o\\s*r\\s*h\\s*u\\s*g\\s*o'
        :param term: str
        """
        term = cls.normalize(term).replace(' ', '')
        term = '\s*'.join(term)
        return re.compile(term)

    def find(self, text):
        term = self.search_term(text)
        return re.finditer(term, self.text)

    def score(self, text):
        found = self.find(text)
        results = []
        for m in found:
            dist = min(list(map(lambda n: min([abs(n - i) for i in self.indices]), m.span())))
            results.append(Score(text=text, min_distance=dist, match=m.group(0)))

        return results

    def scores(self, lookups):
        if not len(self.indices):
            return {}
        result = dict()
        for k, v in lookups.items():
            score = self.score(v)
            logger.debug('score = %s' % str(score))
            if len(score):
                amount = len(score)
                min_distance = min(s.min_distance for s in score)
                result[k] = Scores(amount,
                                   min_distance,
                                   set(x.match for x in score),
                                   amount/min_distance)
        return result


class Rater:
    # todo
    multipliers = defaultdict(lambda: 1,
                              # died_place_locality=5,
                              # born_place_locality=5,
                              # died_place_topo=4,
                              )

    def __init__(self, pid, nmlid, mh=None):
        self.pid = pid
        self.nmlid = nmlid
        self.mh = MediaHaven() if mh is None else mh
        self._language = None
        self._alto = None
        self._details = None
        self._lookups = None

    @property
    def language(self):
        if self._language is None:
            with timeit('mh.one', 200):
                language = self.mh.one('+(externalId:%s)' % self.pid)
                self._language = language['mdProperties']['language'][0].lower()
        return self._language

    @property
    def alto(self):
        if self._alto is None:
            with timeit('alto', 200):
                self._alto = self.mh.get_alto(self.pid)
        return self._alto

    @property
    def details(self):
        if self._details is None:
            with timeit('nml', 100):
                self._details = Namenlijst().get_person_full(self.nmlid, self.language)
        return self._details

    @property
    def lookups(self):
        if self._lookups is None:
            nml = self.details
            self._lookups = {}

            def addlookup(name, f):
                try:
                    val = f()
                    if len(val) < 2:
                        return
                    self._lookups[name] = val
                except KeyError:
                    pass

            addlookup('died_place_locality', lambda: nml.died_place['locality'])
            addlookup('born_place_locality', lambda: nml.born_place['locality'])
            addlookup('died_place_topo', lambda: nml.events['died']['topo'])
            addlookup('profession', lambda: nml.events['work']['work_profession'])
            addlookup('employer', lambda: nml.events['work']['work_company_name'])
            addlookup('school_topo', lambda: nml.events['school']['topo'])
            addlookup('school_name', lambda: nml.events['school']['school_name'])

        return self._lookups

    def ratings(self):
        language, alto, nml = self.language, self.alto, self.details
        matcher = Matcher(alto.text, nml.names.variations_normalized)
        scores = matcher.scores(self.lookups)

        total = sum(scores[k].rating for k in scores)
        return Rating(scores, total)
