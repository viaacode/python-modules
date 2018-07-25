from sickle import Sickle
from sickle.models import Record
import logging
from collections import defaultdict
import re

logger = logging.getLogger(__name__)


def xml_to_dict(t, strip_ns=True):
    """
    Completely map xml elements to a dict (supports any namespaces, preferable to the xml_to_dict provided by Sickle)

    based on https://stackoverflow.com/questions/7684333/converting-xml-to-dictionary-using-elementtree
    :param t:
    :param strip_ns: Whether to strip tagnames from the keys
    :return: dict
    """
    if strip_ns:
        def st(txt):
            return re.sub(r'\{.*\}', '', txt)
    else:
        def st(txt):
            return txt

    d = {st(t.tag): {} if t.attrib else None}
    children = list(t)
    if children:
        dd = defaultdict(list)
        for dc in map(xml_to_dict, children):
            for k, v in dc.items():
                dd[st(k)].append(v)
        d = {st(t.tag): {st(k): v[0] if len(v) == 1 else v for k, v in dd.items()}}
    if t.attrib:
        d[st(t.tag)].update(('@' + st(k), v) for k, v in t.attrib.items())
    if t.text:
        text = t.text.strip()
        if children or t.attrib:
            if text:
                d[st(t.tag)]['#text'] = text
        else:
            d[st(t.tag)] = text
    return d


class OAI(Sickle):
    """
    Child class of Sickle but with altered xml_to_dict functionality for Records (to support other metadataSuffices like
    mets)
    """
    class Record(Record):
        def __init__(self, record_element, strip_ns=True):
            super().__init__(record_element, strip_ns=strip_ns)
            if not self.deleted:
                self.metadata = xml_to_dict(
                    self.xml.find(
                        './/' + self._oai_namespace + 'metadata'
                    ).getchildren()[0], strip_ns=self._strip_ns)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.class_mapping['GetRecord'] = OAI.Record
        self.class_mapping['ListRecords'] = OAI.Record

