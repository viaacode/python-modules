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
        fields = {
            'text': 'CONTENT',
            'x': 'HPOS',
            'y': 'VPOS',
            'w': 'WIDTH',
            'h': 'HEIGHT',
            'color': 'CC',
            'confidence': 'WC'
        }
        return ( AltoWord(zip(fields.keys(), [int(i.attrib[k]) if len(k) == 1 else i.attrib[k] for k in fields.values()])) for i in self._yield_types('String') )

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

        self.xmlns = match[1]

        if self.xmlns not in namespaces:
            raise IndexError("Namspace '%s' not registered" % self.xmlns)

    def pages(self):
        return (AltoPage(page, self.xmlns) for page in self._yield_types('Page'))

class AltoElement(Alto):
    def __init__(self, xml, xmlns):
        super().__init__(xml, xmlns)
        _ = xml.attrib
        self.width = int(_['WIDTH'])
        self.height = int(_['HEIGHT'])
        self.id = _['ID']
        self.dimensions = (self.width, self.height)

class AltoPage(AltoElement):
    def __init__(self, xml, xmlns):
        super().__init__(xml, xmlns)
        _ = xml.attrib
        self.accuracy = float(_['ACCURACY']) if 'ACCURACY' in _ else None
        self.printed_image_number = _['PRINTED_IMG_NR']
        self.physical_image_number = _['PHYSICAL_IMG_NR']

class AltoTextBlock(AltoElement):
    def __init__(self, xml, xmlns):
        super().__init__(xml, xmlns)
        _ = xml.attrib
        self.x = int(_['HPOS'])
        self.y = int(_['VPOS'])

class AltoWord(dict):
    pass
