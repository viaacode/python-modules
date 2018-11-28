from xml.etree import ElementTree
import re
from functools import partial
from copy import copy
import logging
from .namenlijst import Conversions
from collections import namedtuple
from itertools import chain

logger = logging.getLogger(__name__)


class AltoError(Exception):
    pass


class Alto:
    def __init__(self, xml, xmlns):
        self.xml = xml
        self.xmlns = xmlns

    def iterfind(self, q, *args, **kwargs):
        q = './/{%s}%s' % (self.xmlns, q)
        return self.xml.iterfind(q, *args, **kwargs)

    def oa(self, q, k, as_type=float, *args, **kwargs):
        attrib = next(self.iterfind(q, *args, **kwargs)).attrib[k]
        if as_type is not None:
            attrib = as_type(attrib)
        return attrib

    def _yield_types(self, type_name):
        return self.iterfind(type_name)

    def textblocks(self):
        return (AltoTextBlock(block, self.xmlns) for block in self._yield_types('TextBlock'))

    def words(self, parent='TextBlock'):
        return (AltoWord(i, self) for i in self._yield_types('String'))

    @property
    def text(self):
        words = []
        prev_word = None
        for w in self.words():
            if prev_word is not None and prev_word.full_text == w.full_text and w.orig_hyphenated:
                words.pop()
            words.append(w.full_text)
            prev_word = w
        return ' '.join(words)


class AltoRoot(Alto):
    def __init__(self, xml, url=None):
        self.url = url
        self.xml = ElementTree.fromstring(xml)

        # validate namespace
        namespaces = [
            'http://schema.ccs-gmbh.com/ALTO',
            'http://www.loc.gov/standards/alto/ns-v2#',
            'http://www.loc.gov/standards/alto/ns-v3#'
        ]

        match = re.search(r'\{([^}]+)\}', str(self.xml.tag))

        if not match:
            raise IndexError("No namespace found: %s" % self.xml.tag)

        self.xmlns = match.group(1)

        if self.xmlns not in namespaces:
            raise IndexError("Namespace '%s' not registered" % self.xmlns)

    def pages(self):
        return (AltoPage(page, self.xmlns) for page in self._yield_types('Page'))

    def search_words(self, words, search_kind=None):
        if type(words) is str:
            words = [[words]]

        words = list(words)

        if search_kind is None:
            search_kind = 'normalized'
        page = list(self.pages())
        if len(page) != 1:
            # logger.warning("Expected only 1 page, got %d", len(page))
            raise AltoError("Expected only 1 page, got %d" % len(page))
        page = page[0]
        textblocks_extent = None
        words_extent = None

        results = []
        textblocks = []
        for textblock in page.textblocks():
            textblock_extent = Extent.from_object(textblock)
            textblocks.append(textblock_extent)

        alto_words = list(chain.from_iterable(t.words() for t in page.textblocks()))
        # logger.info(alto_words)

        rects = SearchKinds.multi_run(search_kind, alto_words, words)

        for rect in rects:
            word_extent = Extent.from_object(rect)
            textblock = rect.parent
            textblock_extent = Extent.from_object(textblock)

            results.append({
                "extent": word_extent,
                "word": rect,
                'textblock': textblock,
                "extent_textblock": textblock_extent,
                # "orig_phrase": ' '.join(words)
            })
            words_extent = Extent.extend(word_extent, words_extent)
            textblocks_extent = Extent.extend(textblocks_extent, textblock_extent)

        pagedim = page.dimensions

        printspace = Extent(page.oa('PrintSpace', 'HPOS'),
                            page.oa('PrintSpace', 'VPOS'),
                            page.oa('PrintSpace', 'WIDTH'),
                            page.oa('PrintSpace', 'HEIGHT'))

        margins = Extent(page.oa('LeftMargin', 'WIDTH'),
                         page.oa('TopMargin', 'HEIGHT'),
                         page.oa('RightMargin', 'WIDTH'),
                         page.oa('BottomMargin', 'HEIGHT'))

        # dirty hack correction for pages with wrong alto.xml coordinates
        if printspace.w * 2 < pagedim[0]:
            pagedim = list(map(lambda x: x // 2, pagedim))
            correction_factor = 2
        else:
            correction_factor = 1

        return Words({
            "correction_factor": correction_factor,
            "page_dimensions": pagedim,
            "words": results,
            "extent_words": words_extent,
            "extent_textblocks": textblocks_extent,
            "textblocks": textblocks,
            "margins": margins,
            'printspace': printspace,
        })


class AltoElement(Alto):
    def __init__(self, xml, xmlns):
        super().__init__(xml, xmlns)
        self.xml = xml
        _ = xml.attrib
        self.w = float(_['WIDTH'])
        self.h = float(_['HEIGHT'])
        self.id = _['ID']
        self.dimensions = (self.w, self.h)

    def _asdict(self):
        return self.__dict__


class AltoPage(AltoElement):
    def __init__(self, xml, xmlns):
        super().__init__(xml, xmlns)
        _ = xml.attrib
        self.accuracy = float(_['ACCURACY']) if 'ACCURACY' in _ else None
        self.printed_image_number = _['PRINTED_IMG_NR'] if 'PRINTED_IMG_NR' in _ else None
        self.physical_image_number = _['PHYSICAL_IMG_NR'] if 'PHYSICAL_IMG_NR' in _ else None


class AltoTextBlock(AltoElement):
    def __init__(self, xml, xmlns):
        super().__init__(xml, xmlns)
        _ = xml.attrib
        self.x = float(_['HPOS'])
        self.y = float(_['VPOS'])


class AltoWord(object):
    fields = {
        'text': 'CONTENT',
        'x': 'HPOS',
        'y': 'VPOS',
        'w': 'WIDTH',
        'h': 'HEIGHT',
        'color': 'CC',
        'confidence': 'WC',
        'id': 'ID',
        'orig_hyphenated': None,
        'meta': None
    }

    def __init__(self, xml, parent):
        # AltoWordFields = namedtuple('AltoWordFields', fields.keys())
        self._parent = parent

        self.xml = xml

        for k, attr in self.fields.items():
            if attr is None:
                continue
            v = None
            if attr in xml.attrib:
                v = xml.attrib[attr]
            if len(k) == 1:
                v = float(v)
            setattr(self, k, v)

        self.orig_hyphenated = 'SUBS_CONTENT' in xml.attrib
        if self.orig_hyphenated:
            self.full_text = xml.attrib['SUBS_CONTENT']
        else:
            self.full_text = self.text

        self.meta = None

    @property
    def parent(self):
        return self._parent

    def _asdict(self):
        return {k: getattr(self, k) for k in self.fields.keys()}

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, self.__str__())

    def __str__(self):
        return str(self.__dict__)


class AltoNull:
    def words(self):
        return ()

    def pages(self):
        return ()

    def textblocks(self):
        return ()


def set_obj_key(key, value, obj):
    obj = copy(obj)
    setattr(obj, key, value)
    return obj


class SearchKinds:
    @staticmethod
    def multi_run(kind, words, list_of_tocheck, **kwargs):
        result = []
        # list_of_tocheck = [x for x in set(tuple(x) for x in list_of_tocheck)]
        # logger.debug('to check: %s', list_of_tocheck)
        for tocheck in list_of_tocheck:
            # logger.debug('Check "%s"', tocheck)
            result.extend(SearchKinds.run(kind, words, tocheck, **kwargs))
        return result

    @staticmethod
    def run(kind, words, tocheck, **kwargs):
        if tocheck is None:
            tocheck = []
        method = getattr(SearchKinds, kind) if type(kind) is str else kind
        result = method([word for word in words if type(word.full_text) is str], tocheck, **kwargs)
        if len(result):
            addsearchphrase = partial(set_obj_key, 'meta', ' '.join(tocheck))
            result = list(map(addsearchphrase, result))
        return result

    @staticmethod
    def normalized(words, tocheck):
        res = []
        normalizer = Conversions.normalize_no_num
        tocheck = list(map(normalizer, tocheck))
        text = ((idx, normalizer(w.full_text), w.orig_hyphenated) for idx, w in enumerate(words) if len(normalizer(w.full_text)))

        # todo: make sure that both are included, now if dupe word, only all added if it is right side, not left side:
        # eg. correct: mike de de smet
        #   incorrect: Eugene Eugene Deloge
        def remove_dups(a_list):
            l = []
            prev = (0, '', False)
            deb = 0
            for item in a_list:
                # if item[1] == 'paul':
                #     deb = 5
                #
                # if deb > 0:
                #     deb -= 1
                #     logger.info('%s', item)
                if len(item[1]) == 0:
                    continue
                if prev[2] and prev[1] == item[1]:
                    l.pop()
                prev = item
                l.append(item)
            return l
        text = remove_dups(text)

        l = len(tocheck)

        for idx in range(len(text)-l+1):
            # logger.info('Compare %d:%d %s to %s', idx, idx+l,
            #             ' '.join(tocheck), ' '.join(t[1] for t in text[idx:idx+l]))
            if all(nextcheck == text[idx + nextcheckidx][1] for nextcheckidx, nextcheck in enumerate(tocheck)):
                # logger.warning('FOUND %d %s', idx, [w.full_text for w in words[text[idx][0]:text[idx+l-1][0]+1]])
                res.extend(words[text[idx][0]:text[idx+l-1][0]+1])
        return res

    @staticmethod
    def containsproximity(words, tocheck, proximity=2):
        res = []
        for idx, word in enumerate(words):
            if any(c in word.full_text for c in tocheck):
                # check proximity
                context = [c.full_text for c in words[idx-proximity:idx+proximity+1]]
                if all(any(tocheckword in c for c in context) for tocheckword in tocheck):
                    res.append(word)
        return res

    @staticmethod
    def contains(words, tocheck):
        res = []
        for w in tocheck:
            res.extend([word for word in words if w in word.full_text])
        return res

    @staticmethod
    def icontains(words, tocheck):
        res = []
        for w in tocheck:
            w = w.lower()
            res.extend([word for word in words if w in word.full_text.lower()])
        return res

    @staticmethod
    def literal(words, tocheck):
        res = []
        for w in tocheck:
            res.extend([word for word in words if w == word.full_text])
        return res

    @staticmethod
    def iliteral(words, tocheck):
        res = []
        for w in tocheck:
            w = w.lower()
            res.extend([word for word in words if w == word.full_text.lower()])
        return res


class Extent:
    def __init__(self, x, y, w, h):
        self.x = float(x)
        self.y = float(y)
        self.w = float(w)
        self.h = float(h)

    def as_coords(self):
        return [(self.x, self.y), (self.x + self.w, self.y + self.h)]

    def as_box(self):
        return self.x, self.y, self.x + self.w, self.y + self.h

    def _asdict(self):
        return copy(self.__dict__)

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, self.__str__())

    def scale(self, scale_x, scale_y=None, inplace=False):
        if scale_y is None:
            scale_y = scale_x

        if not inplace:
            return Extent(self.x * scale_x, self.y * scale_y,
                          self.w * scale_x, self.h * scale_y)
        self.x *= scale_x
        self.y *= scale_y
        self.w *= scale_x
        self.h *= scale_y
        return self

    def pad(self, padding_x, padding_y=None, inplace=False):
        if padding_y is None:
            padding_y = padding_x
        if not inplace:
            return Extent(self.x + padding_x, self.y + padding_y,
                          self.w - 2*padding_x, self.h - 2*padding_y)
        self.x += padding_x
        self.w -= 2*padding_x
        self.y += padding_y
        self.h -= 2*padding_y
        return self

    @staticmethod
    def from_object(_):
        return Extent(_.x, _.y, _.w, _.h)

    @staticmethod
    def from_dict(_):
        return Extent(_['x'], _['y'], _['w'], _['h'])

    @staticmethod
    def from_rect(x, y, w, h):
        return Extent(x, y, w, h)

    @staticmethod
    def from_coords(_):
        (x1, y1) = _[0]
        (x2, y2) = _[1]
        return Extent(x1, y1, x2 - x1, y2 - y1)

    @staticmethod
    def extend(*extents):
        func = (min, max)
        args = [Extent.from_object(ext).as_coords() for ext in extents if ext is not None]

        (x1, y1, x2, y2) = (
            func[0]([a[0][0] for a in args]),
            func[0]([a[0][1] for a in args]),
            func[1]([a[1][0] for a in args]),
            func[1]([a[1][1] for a in args]),
        )

        return Extent.from_coords([(x1, y1), (x2, y2)])


class Words(dict):
    pass
