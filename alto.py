import xml.etree.ElementTree as ET
import re


class Alto:
    def __init__(self, xml, xmlns):
        self.xml = xml
        self.xmlns = xmlns

    def iterfind(self, q, *args, **kwargs):
        q = './/{%s}%s' % (self.xmlns, q)
        return self.xml.iterfind(q, *args, **kwargs)

    def _yield_types(self, type_name):
        return self.iterfind(type_name)

    def textblocks(self):
        return (AltoTextBlock(block, self.xmlns) for block in self._yield_types('TextBlock'))

    def words(self):
        return ( AltoWord(i) for i in self._yield_types('String') )

class AltoRoot(Alto):
    def __init__(self, xml):
        self.xml = ET.fromstring(xml)

        # validate namespace
        namespaces = [
            'http://schema.ccs-gmbh.com/ALTO',
            'http://www.loc.gov/standards/alto/ns-v2#',
            'http://www.loc.gov/standards/alto/ns-v3#'
        ]

        match = re.search(r'\{([^}]+)\}', str(self.xml.tag))

        if not match:
            raise IndexError("No namespace found")

        self.xmlns = match.group(1)

        if self.xmlns not in namespaces:
            raise IndexError("Namespace '%s' not registered" % self.xmlns)

    def pages(self):
        return (AltoPage(page, self.xmlns) for page in self._yield_types('Page'))

class AltoElement(Alto):
    def __init__(self, xml, xmlns):
        super().__init__(xml, xmlns)
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
            'confidence': 'WC'
        }

        for k, attr in fields.items():
            v = None
            if attr in xml.attrib:
                v = xml.attrib[attr]
            if v is None:
                raise Exception("Couldnt find value for '%s' in %s" % (attr, xml.attrib))
            if len(k) == 1:
                v = int(v)
            setattr(self, k, v)


class AltoNull:
    def words(self):
        return ()
    def pages(self):
        return ()
    def textblocks(self):
        return ()
