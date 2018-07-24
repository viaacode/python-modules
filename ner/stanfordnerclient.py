from pythonmodules.ner import NER
from sner import Ner
import itertools


class StanfordNERClient(NER):
    """
    To run the server see:
        pythonmodules/ner/run_stanford.sh
    """
    def __init__(self, host=None, port=None):
        self.host = 'localhost' if host is None else host
        self.port = 9001 if port is None else port
        self.server = None

    def connect(self):
        self.server = Ner(host=self.host, port=self.port)

    def tag(self, text, **kwargs):
        # remove "untokenizable" characters to avoid warning from ner server
        text = bytes(text, 'utf-8').decode('utf-8', 'ignore')
        text = text.replace('\xFF\xFD', '')

        text = str(text).splitlines()
        if self.server is None:
            self.connect()
        try:
            return self._run(text)
        except ConnectionResetError:
            self.connect()
        except ConnectionRefusedError:
            raise ConnectionRefusedError(
                "Connection refused, is the server running at %s:%d? Check run_stanford.sh..." % (self.host, self.port))
        return self._run(text)

    def _run(self, text):
        return list(itertools.chain(*[self.server.get_entities(line) for line in text]))

