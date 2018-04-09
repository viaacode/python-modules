# Usage:
# mm = MediaHaven([config])
# By default it will read config.ini from the current working directory and
# use that config.
# You can also use a dict with keys:
#    - user
#    - pass
#    - host (eg. archief-qas.viaa.be)
#    - port (eg. 443)
#    - basePath (eg. /mediahaven-rest-api)
#    - protocol (eg. 'https')


import requests as req
import logging
import http.client as http_client
import configparser
import urllib
import time
from . import alto
from PIL import Image, ImageDraw
from io import BytesIO


class MediaHavenException(Exception):
    pass

class MediaHaven:
    def __init__(self, config = None):
        if config == None:
            config = 'config.ini'
        if type(config) == str:
            config = configparser.ConfigParser()
            config.read('config.ini')
        try:
            config = config['mediahaven']
        except Exception:
            pass
        self.config = config
        self.URL = config['protocol'] + '://' + config['host'] + ':' + str(config['port']) + config['basePath']
        self.token = None
        self.tokenText = None

    def refresh_token(self):
        """Fetch a new token based on the user/pass combination of config
        """
        conf = self.config
        r = req.post(self.URL + '/resources/oauth/access_token', auth=(conf['user'], conf['pass']), data={'grant_type': 'password'})
        self._validate_response(r)
        self.token = r.json()
        self.tokenText = self.token['token_type'] + ' ' + self.token['access_token']

    def _validate_response(self, r, status_code = 200):
        if r.status_code < 200 or r.status_code >= 300:
            raise MediaHavenException("Wrong status code " + str(r.status_code) + ": " + r.text)

    def call_absolute(self, url, params = {}, method = None, raw_response = False):
        if method == None:
            method = 'get'

        def do_call():
            if not self.tokenText:
                self.refresh_token()
            return getattr(req, method)(url, headers={'Authorization': self.tokenText}, params = params)

        r = do_call()
        if r.status_code < 200 or r.status_code >= 300:
            self.refresh_token()
            r = do_call()
        self._validate_response(r)
        if raw_response:
            return r
        return r.json()

    def call(self, url, *args, **kwargs):
        """Execute a call to MediaHaven server
        """
        return self.call_absolute(self.URL + url, *args, **kwargs)

    def search(self, q, startIndex = 0, nrOfResults = 25):
        """Execute a mediahaven search query
        """
        return SearchResultIterator(self, q, startIndex, nrOfResults)

    def set_log_http_requests(self, enabled = False):
        """Toggle logging of http requests
        """
        http_client.HTTPConnection.debuglevel = 1 if enabled else 0
        logLevel = logging.DEBUG if enabled else logging.WARNING

        logging.basicConfig()
        logging.getLogger().setLevel(logLevel)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(logLevel)
        requests_log.propagate = enabled

    def media(self, mediaObjectId, action = None, *args, **kwargs):
        """Do a media query
        """
        url = '/resources/media/%s' % urllib.parse.quote_plus(mediaObjectId)
        if action:
            url = '%s/%s' % (url, action)
        if action in ['fragments']:
            return MediaDataListIterator(self, url = url)
        return self.call(url, *args, **kwargs)

    def export(self, mediaObjectId):
        """Export a file
        """
        res = self.media(mediaObjectId, 'export', method='post', raw_response = True)
        return Export(self, res.headers['Location'], res.json())

    def get_alto(self, pid, timeout = 1):
        res = self.search('+(originalFileName:%s_alto.xml)' % pid)
        if len(res) == 0:
            return None
        if len(res) > 1:
            raise "Expected only one result"

        res = next(res)
        mediaObjectId = res['mediaObjectId']
        export = self.export(mediaObjectId)
        timeout *= 10
        while timeout >= 0 and not export.is_ready():
            timeout += 1
            time.sleep(0.1)
        files = export.get_files()
        if files == None or len(files) != 1:
            raise "Couldn't get files..."
        return alto.AltoRoot(req.get(files[0]).content)

    def fragments(self, mediaObjectId):
        """Get fragments for a media object
        """
        return self.media(mediaObjectId, 'fragments')

    def get_preview(self, pid):
        """Get a preview of an item (fetches previewImagePath for pid)
        """
        return PreviewImage(pid, self)

class PreviewImage:
    def __init__(self, pid, mh):
        self.mh  = mh
        self.pid = pid
        self.meta = None
        self.image = None
        self.closed = True
        #self.__enter__()

    def __enter__(self):
        return self.open()

    def __exit__(self, *args, **kwargs):
        return self.close()

    def open(self):
        if not self.closed:
            raise IOError("File already open")

        items = self.mh.search('+(externalId:%s)' % self.pid)
        if len(items) == 0:
            return None
        if len(items) > 1:
            raise "Expected only 1 result"
        self.meta = next(items)
        self.image = Image.open(BytesIO(req.get(self.meta['previewImagePath']).content))
        self.closed = False
        return self

    def close(self):
        if self.closed:
            raise IOError("Cannot close a not-open file")
        self.image.close()
        self.closed = True
        return self

    def highlight_words(self, words, search_kind = None, color = (255, 255, 0)):
        if self.closed:
            raise IOError("Cannot work on a closed file")
        if search_kind is None:
            search_kind = 'icontains'
        if type(words) is str:
            words = [words]
        alto = self.mh.get_alto(self.pid)
        self.alto = alto
        pages = list(alto.pages())
        if len(pages) != 1:
            raise "Expected only 1 page, got %d" % len(pages)

        im = self.image
        p = pages[0]
        (w, h) = p.dimensions
        rects = []
        for tocheck in words:
            if search_kind[0] == 'i':
                tocheck = tocheck.lower()
            rects.extend(getattr(SearchKinds, search_kind)(alto.words(), tocheck))

        scale_x = im.size[0] / w
        scale_y = im.size[1] / h
        padding = 2
        canvas = ImageDraw.Draw(im)
        for rect in rects:
            coords = [
                (
                    int(rect['x']) * scale_x - padding,
                    int(rect['y']) * scale_y - padding
                ),
                (
                    (int(rect['x']) + int(rect['w'])) * scale_x + padding,
                    (int(rect['y']) + int(rect['h'])) * scale_y + padding
                )
            ]
            canvas.rectangle(coords, outline=color)


class Export:
    def __init__(self, mh, location, result):
        self.mh = mh
        self.location = location
        self.result = result
        self.files = []

    def _do_req(self):
        return self.mh.call_absolute(self.location)

    def is_ready(self):
        if len(self.files) == len(self.result):
            return True
        res = self._do_req()
        self.files = [status['downloadUrl'] for status in res if status['status'] == 'completed']
        return len(self.files) == len(self.result)

    def get_files(self):
        if self.is_ready():
            return self.files
        return None

    def get_status(self):
        return [file['status'] for file in self._do_req()]

    def fetch_files(self):
        if not self.is_ready():
            raise MediaHavenException("Not ready yet")
        files = self.get_files()

        data = []
        for file in files:
            res = req.get(file)
            if res.status_code != req.codes.ok:
                raise MediaHavenException("Invalid status code %d" % res.status_code)
            data.append(res.content)
        return data



class MediaDataListIterator:
    def __init__(self, mh, params = {}, url = '/resources/media', start_index = 0, buffer_size = 25, param_map = None):
        self.buffer_size = buffer_size
        self.mh = mh
        self.params = params
        self.url = url
        self.length = None
        self.buffer = []
        self.i = start_index
        self.buffer_idx = 0
        if param_map != None:
            self.param_map = param_map
        else:
            self.param_map = {
                "i": "startIndex",
                "buffer_size": "nrOfResults"
            }

    def __iter__(self):
         return self

    def fetch_next(self):
        for (k, v) in self.param_map.items():
            self.params[v] = getattr(self, k)

        results = self.mh.call(self.url, self.params)

        if self.length != None and self.length != results['totalNrOfResults']:
            raise MediaHavenException("Difference in length, had %d, now getting %d" % (self.length, results['totalNrOfResults']))

        self.length = results['totalNrOfResults']
        self.buffer = results['mediaDataList']

    def __len__(self):
        if self.length == None:
            self.fetch_next()
        return self.length

    def __next__(self):
        if self.length == None:
            self.fetch_next()

        if self.i >= self.length:
            raise StopIteration()

        self.i += 1
        self.buffer_idx += 1

        if (self.buffer_idx >= len(self.buffer) and self.i < self.length):
            self.buffer_idx = 0
            self.fetch_next()

        return self.buffer[self.buffer_idx - 1]

    def set_buffer_size(self, buffer_size):
        self.buffer_size = buffer_size

class SearchResultIterator(MediaDataListIterator):
    def __init__(self, mh, q, start_index = 0, buffer_size = 25):
        super().__init__(mh, params = { "q": q }, start_index = start_index, buffer_size = buffer_size)

class SearchKinds:
    def contains(words, tocheck):
        return [word for word in words if tocheck in word['text']]
    def icontains(words, tocheck):
        return [word for word in words if tocheck in word['text'].lower()]
    def literal(words, tocheck):
        return [word for word in words if tocheck == word['text']]
    def iliteral(words, tocheck):
        return [word for word in words if tocheck == word['text'].lower()]
