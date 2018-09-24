from xml.etree import ElementTree
import re
from functools import partial
from copy import copy
import logging

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

    def oa(self, q, k, *args, **kwargs):
        return int(next(self.iterfind(q, *args, **kwargs)).attrib[k])

    def _yield_types(self, type_name):
        return self.iterfind(type_name)

    def textblocks(self):
        return (AltoTextBlock(block, self.xmlns) for block in self._yield_types('TextBlock'))

    def words(self):
        return (AltoWord(i) for i in self._yield_types('String'))

    @property
    def text(self):
        return ' '.join(word.full_text for word in self.words())


class AltoRoot(Alto):
    def __init__(self, xml):
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
            search_kind = 'icontainsproximity'
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
            rects = SearchKinds.multi_run(search_kind, list(textblock.words()), words)

            for rect in rects:
                word_extent = Extent.from_object(rect)
                results.append({
                    "extent": word_extent,
                    "word": rect,
                    'textblock': textblock,
                    "extent_textblock": textblock_extent,
                    # "orig_phrase": ' '.join(words)
                })
                words_extent = Extent.extend(word_extent, words_extent)

            if len(rects):
                textblocks_extent = Extent.extend(textblock_extent, textblocks_extent)

        pagedim = page.dimensions

        # printspace = next(page.iterfind('PrintSpace')).attrib
        # pagedim = (int(printspace['WIDTH']), int(printspace['HEIGHT']))

        # pagedim[0] += int(page.one_attrib('TopMargin')['HPOS'])
        # pagedim[1] += int(page.one_attrib('LeftMargin')['VPOS'])

        return Words({
            "page_dimensions": pagedim,
            "words": results,
            "extent_words": words_extent,
            "extent_textblocks": textblocks_extent,
            "textblocks": textblocks,
            "ext2": Extent(page.oa('LeftMargin', 'HPOS'),
                           page.oa('TopMargin', 'VPOS'),
                           page.oa('RightMargin', 'HPOS'),
                           page.oa('BottomMargin', 'VPOS'))
            # "words_topleft": (min_x, min_y),
        })


class AltoElement(Alto):
    def __init__(self, xml, xmlns):
        super().__init__(xml, xmlns)
        self.xml = xml
        _ = xml.attrib
        self.w = int(_['WIDTH'])
        self.h = int(_['HEIGHT'])
        self.id = _['ID']
        self.dimensions = (self.w, self.h)


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
        self.x = int(_['HPOS'])
        self.y = int(_['VPOS'])


class AltoWord(object):
    def __init__(self, xml):
        fields = {
            'text': 'CONTENT',
            'x': 'HPOS',
            'y': 'VPOS',
            'w': 'WIDTH',
            'h': 'HEIGHT',
            'color': 'CC',
            'confidence': 'WC',
            'id': 'ID',
        }

        self.xml = xml
        for k, attr in fields.items():
            v = None
            if attr in xml.attrib:
                v = xml.attrib[attr]
            if len(k) == 1:
                v = int(v)
            setattr(self, k, v)
        if 'SUBS_CONTENT' in xml.attrib:
            self.full_text = xml.attrib['SUBS_CONTENT']
        else:
            self.full_text = self.text
        self.meta = None

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
            logger.debug('Check "%s"', tocheck)
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
    def icontainsproximity(words, tocheck, proximity=2):
        res = []
        tocheck = list(map(str.lower, tocheck))
        for idx, word in enumerate(words):
            if any(c in word.full_text.lower() for c in tocheck):
                # check proximity
                context = [c.full_text.lower() for c in words[idx-proximity:idx+proximity+1]]
                if all(any(tocheckword in c for c in context) for tocheckword in tocheck):
                    res.append(word)
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

    def __str__(self):
        return str(self.__dict__)

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, self.__str__())

    def scale(self, scale_x, scale_y, inplace=False):
        if not inplace:
            return Extent(self.x * scale_x, self.y * scale_y, self.w * scale_x, self.h * scale_y)
        self.x *= scale_x
        self.y *= scale_y
        self.w *= scale_x
        self.h *= scale_y
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
