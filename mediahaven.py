# Usage:
# mm = MediaHaven([config])
# By default it will read config.ini from the current working directory and
# use that config.
# You can also use a dict with keys:
#    - user
#    - pass
#    - host (eg. archief-qas.viaa.be)
#    - port (eg. 443)
#    - base_path (eg. /mediahaven-rest-api)
#    - protocol (eg. 'https')


import requests as req
import logging
import http.client as http_client
import urllib
import time
from . import alto
from .config import Config
from .decorators import cache, memoize, classcache, DictCacher
from PIL import Image, ImageDraw
from io import BytesIO
logger = logging.getLogger(__name__)

class MediaHavenException(Exception):
    pass


class MediaHaven:
    def __init__(self, config = None):
        self.config = Config(config, 'mediahaven')
        _ = self.config
        self.URL = '%s://%s:%s%s' % (_['protocol'], _['host'], str(_['port']), _['base_path'])
        self.token = None
        self.tokenText = None

        self.__cache = DictCacher()
        if 'cache' in _:
            self.__cache = _['cache']

        if 'debug' in self.config and self.config['debug']:
            logging.basicConfig()
            logger.setLevel(logging.DEBUG)
            logger.propagate = True
            logger.debug('Debugging enabled through configuration')


    def get_cache(self):
        return self.__cache

    def set_cache(self, cache):
        self.__cache = cache
        return self

    def oai(self):
        from sickle import Sickle
        _ = self.config
        url = '%s://%s:%s%s' % (_['protocol'], _['host'], str(_['port']), _['oai_path'])
        return Sickle(url, auth=(_['user'], _['pass']))

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
            logger.warn("Wrong status code %d: %s " % (r.status_code, r.text))
            raise MediaHavenException("Wrong status code %d: %s " % (r.status_code, r.text))

    def call_absolute(self, url, params = None, method = None, raw_response = False):
        if method == None:
            method = 'get'

        def do_call():
            if not self.tokenText:
                self.refresh_token()
            res = getattr(req, method)(url, headers={'Authorization': self.tokenText}, params = params)
            return res

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

    def one(self, q):
        """Execute a mediahaven search query, return first result (or None)
        """
        params = {
            "q": q,
            "startIndex": 0,
            "nrOfResults": 1
        }
        res = self.call('/resources/media/', params)
        if not res:
            return None
        return res['mediaDataList'][0]

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

    def export(self, mediaObjectId, reason = None):
        """Export a file
        """
        params = { "exportReason": reason } if reason else None
        res = self.media(mediaObjectId, 'export', method='post', raw_response = True, params = params)
        return Export(self, res.headers['Location'], res.json())

    @classcache
    def get_alto(self, pid, max_timeout = None):
        logger.debug('getting alto for %s ' % pid)
        res = self.search('+(originalFileName:%s_alto.xml)' % pid)

        if len(res) == 0:
            logger.warning('Expected 1 result for %s, got none' % pid)
            return None

        if len(res) > 1:
            logger.warning('Expected 1 result for %s, gotten %d' % (pid, len(res)))
            raise MediaHavenException("Expected only one result")

        res = next(res)
        mediaObjectId = res['mediaObjectId']
        export = self.export(mediaObjectId)
        if max_timeout is None:
            max_timeout = 5
        repeats = max_timeout * 10
        logger.debug("Get export for %s" % mediaObjectId)
        while (repeats > 0) and (not export.is_ready()):
            repeats -= 1
            time.sleep(0.1)

        if not export.is_ready():
            msg = "Timeout of %ds reached without export being ready for '%s'" % (max_timeout, export.location)
            logger.warning(msg)
            raise MediaHavenException(msg)

        files = export.get_files()
        if files is None or len(files) != 1:
            logger.warning("Couldn't get files. %s" % ('None' if files is None else ('Length: %d' % len(files))))
            raise MediaHavenException("Couldn't get files...")

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
        self.closed = False
        if len(items) == 0:
            return None
        if len(items) > 1:
            raise MediaHavenException("Expected only 1 result")
        self.meta = next(items)
        self.image = Image.open(BytesIO(req.get(self.meta['previewImagePath']).content))
        return self

    def close(self):
        if self.closed:
            raise IOError("Cannot close a not-open file")
        if self.image:
            self.image.close()
        self.closed = True
        return self

    def highlight_confidence(self, im = None, max_timeout=None):
        if self.closed:
            raise IOError("Cannot work on a closed file")

        alto = self.get_alto()
        pages = list(alto.pages())
        if len(pages) != 1:
            raise MediaHavenException("Expected only 1 page, got %d" % len(pages))

        if im is None:
            im = self.image.copy()

        p = pages[0]
        (w, h) = p.dimensions

        min_x = None
        min_y = None
        scale_x = im.size[0] / w
        scale_y = im.size[1] / h
        padding = 0
        canvas = ImageDraw.Draw(im)
        logger.debug('page: %dx%d, image: %dx%d, scale: %1.2fx%1.2f' % (w, h, im.size[0], im.size[1], scale_x, scale_y))

        for rect in p.words():
            if rect.x is None:
                logger.warning("Rect with missing x: %s" % rect.__dict__)
                continue
            x0 = int(rect.x) * scale_x - padding
            y0 = int(rect.y) * scale_y - padding
            x1 = (int(rect.x) + int(rect.w)) * scale_x + padding
            y1 = (int(rect.y) + int(rect.h)) * scale_y + padding
            if min_x is None or x0 < min_x:
                min_x = x0
            if min_y is None or y0 < min_y:
                min_y = y0
            color = 0 if rect.confidence is None else int((1-float(rect.confidence))*255)
            canvas.rectangle([(x0, y0), (x1, y1)], outline=(color, 255 - color, 0))
        return im

    def get_alto(self):
        return self.mh.get_alto(self.pid)

    #@memoize
    def get_words(self, words, search_kind = None):
        if type(words) is str:
            words = [words]
        if search_kind is None:
            search_kind = 'icontains'
        alto = self.get_alto()
        page = list(alto.pages())
        if len(page) != 1:
            raise MediaHavenException("Expected only 1 page, got %d" % len(page))
        page = page[0]
        textblocks_extent = None
        words_extent = None

        results = []
        coords = []
        textblocks = []
        for textblock in page.textblocks():
            textblock_extent = Extent.from_object(textblock)
            textblocks.append(textblock_extent)
            rects = []
            for tocheck in words:
                rects.extend(SearchKinds.run(search_kind, textblock.words(), tocheck))

            for rect in rects:
                word_extent = Extent.from_object(rect)
                coords.append({
                    "extent": word_extent,
                    "word": rect,
                    "extent_textblock": textblock_extent
                })
                words_extent = Extent.extend(word_extent, words_extent)

            if len(rects):
                textblocks_extent = Extent.extend(textblock_extent, textblocks_extent)
                results.extend(coords)

        pagedim = page.dimensions


        # printspace = next(page.iterfind('PrintSpace')).attrib
        # pagedim = (int(printspace['WIDTH']), int(printspace['HEIGHT']))

        # pagedim[0] += int(page.one_attrib('TopMargin')['HPOS'])
        # pagedim[1] += int(page.one_attrib('LeftMargin')['VPOS'])

        return {
            "page_dimensions": pagedim,
            "words": results,
            "extent_words": words_extent,
            "extent_textblocks": textblocks_extent,
            "textblocks": textblocks,
            "ext2": Extent(page.oa('LeftMargin', 'HPOS'), page.oa('TopMargin', 'VPOS'), page.oa('RightMargin', 'HPOS'), page.oa('BottomMargin', 'VPOS'))
            # "words_topleft": (min_x, min_y),
        }


    def highlight_words(self, words, search_kind = None, im = None, crop = True, highlight_textblocks = True):
        color = (255, 255, 0)
        if im is None:
            im = self.image.copy()
        canvas = ImageDraw.Draw(im)
        coords =  self.get_words(words, search_kind = search_kind)
        padding = 2

        (page_w, page_h) = coords['page_dimensions']
        (w, h) = im.size
        scale_x = w / page_w
        scale_y = h / page_h

        logger.debug('Scaling, page: %dx%d vs img %dx%d => %3.2fx%3.2f' % (page_w, page_h, w, h, scale_x, scale_y))

        for word in coords['words']:
            rect = word['extent'].scale(scale_x, scale_y)
            canvas.rectangle(rect.as_coords(), outline=color)

        if highlight_textblocks:
            for textblock_extent in coords['textblocks']:
                canvas.rectangle(textblock_extent.scale(scale_x, scale_y).as_coords(), outline=(0, 0, 255))


        canvas.rectangle(coords['ext2'].scale(scale_x, scale_y).as_coords(), outline=(255, 0, 0))
        if crop:
            im = im.crop(coords['extent_textblocks'].scale(scale_x, scale_y).as_box())
        else:
            canvas.rectangle(coords['extent_textblocks'].scale(scale_x, scale_y).as_coords(), outline=(0, 0, 255))

        pagepad = 30
        # page_w = w
        # page_h = h
        page_w *= scale_x
        page_h *= scale_y
        page_w -= 2*pagepad
        page_h -= 2*pagepad
        pagecoords = [(pagepad, pagepad), (page_w, page_h)]

        canvas.rectangle(pagecoords, outline=(0, 255, 0))
        return im


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
        logger.debug("Export: %d todo, done:\n%s" % (len(self.result), self.files))
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
    def run(kind, words, tocheck):
        if tocheck is None:
            tocheck = ''
        return getattr(SearchKinds, kind)([word for word in words if type(word.text) is str], tocheck)
    def contains(words, tocheck):
        return [word for word in words if tocheck in word.text]
    def icontains(words, tocheck):
        return [word for word in words if tocheck.lower() in word.text.lower()]
    def literal(words, tocheck):
        return [word for word in words if tocheck == word.text]
    def iliteral(words, tocheck):
        return [word for word in words if tocheck.lower() == word.text.lower()]

class Extent:
    def __init__(self, x, y, w, h):
        self.x = float(x)
        self.y = float(y)
        self.w = float(w)
        self.h = float(h)

    def as_coords(self):
        return [(self.x, self.y), (self.x + self.w, self.y + self.h)]

    def as_box(self):
        return (self.x, self.y, self.x + self.w, self.y + self.h)

    def __dict__(self):
        return {
            "x": self.x,
            "y": self.y,
            "w": self.w,
            "h": self.h
        }

    def __str__(self):
        return str(dict(self))

    def scale(self, scale_x, scale_y, inplace = False):
        if not inplace:
            return Extent(self.x * scale_x, self.y * scale_y, self.w * scale_x, self.h * scale_y)
        self.x *= scale_x
        self.y *= scale_y
        self.w *= scale_x
        self.h *= scale_h
        return self

    @staticmethod
    def from_object(_):
        return Extent(_.x, _.y, _.w, _.h)

    @staticmethod
    def from_dict(_):
        return Extent(_['x'], _['y'], _['w'], _['h'])

    @staticmethod
    def from_rect(x, y, w, h):
        return Extent()

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
            func[1]([a[1][0] for a in args]),
        )

        return Extent.from_coords([(x1, y1), (x2, y2)])
