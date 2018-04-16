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
        self.cropped_coords = None
        self.scroll_coords = (0, 0)
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
        crop = [w, h, 0, 0]
        min_x = None
        min_y = None
        scale_x = im.size[0] / w
        scale_y = im.size[1] / h
        padding = 0
        canvas = ImageDraw.Draw(im)
        logger.debug('img: %dx%d, page: %dx%d, scale: %1.2fx%1.2f' % (w, h, im.size[0], im.size[1], scale_x, scale_y))

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

    def get_words(self, words, search_kind = None):
        def format_coords(_):
            x = int(_.x)
            y = int(_.y)
            w = int(_.w)
            h = int(_.h)
            return [(x, y), (x+w, y+h)]

        def calc_extents(*args, func = None):
            if func is None:
                func = (min, max)
            return [
                (
                    func[0]([a[0][0] for a in args]),
                    func[0]([a[0][1] for a in args])
                ),
                (
                    func[1]([a[1][0] for a in args]),
                    func[1]([a[1][1] for a in args])
                )
            ]
        if type(words) is str:
            words = [words]
        if search_kind is None:
            search_kind = 'icontains'
        alto = self.get_alto()
        page = list(alto.pages())
        if len(page) != 1:
            raise MediaHavenException("Expected only 1 page, got %d" % len(page))
        page = page[0]
        (w, h) = page.dimensions
        textblocks_extent = None
        words_extent = None

        results = []
        coords = []
        for textblock in page.textblocks():
            textblock_extent = format_coords(textblock)
            rects = []
            for tocheck in words:
                rects.extend(SearchKinds.run(search_kind, textblock.words(), tocheck))

            for rect in rects:
                coords.append({
                    "extent": format_coords(rect),
                    "word": rect,
                    "extent_textblock": textblock_extent
                })
                words_extent = calc_extents(words_extent, word_extent)

            if len(rects):
                textblocks_extent = calc_extents(textblock_extent, textblocks_extent)
                results.extend(coords)
        self.scroll_coords = (min_x, min_y)
        return {
            "extent_page": format_coords(0, 0, w, h),
            "results": results,
            "extent_words": words_extent,
            "extent_textblocks": textblocks_extent
        }


    def highlight_words(self, words, search_kind = None, im = None, max_timeout=None, crop = True):
        color = (255, 255, 0)
        if self.closed:
            raise IOError("Cannot work on a closed file")
        if search_kind is None:
            search_kind = 'icontains'
        if type(words) is str:
            words = [words]
        alto = self.get_alto()
        pages = list(alto.pages())
        if len(pages) != 1:
            raise MediaHavenException("Expected only 1 page, got %d" % len(pages))
        if im is None:
            im = self.image.copy()
        p = pages[0]
        (w, h) = p.dimensions
        crop = [w, h, 0, 0]
        min_x = None
        min_y = None
        canvas = ImageDraw.Draw(im)
        for textblock in p.textblocks():
            rects = []
            for tocheck in words:
                rects.extend(SearchKinds.run(search_kind, textblock.words(), tocheck))

            scale_x = im.size[0] / w
            scale_y = im.size[1] / h
            padding = 2
            for rect in rects:
                x0 = int(rect.x) * scale_x - padding
                y0 = int(rect.y) * scale_y - padding
                x1 = (int(rect.x) + int(rect.w)) * scale_x + padding
                y1 = (int(rect.y) + int(rect.h)) * scale_y + padding
                if min_x is None or x0 < min_x:
                    min_x = x0
                if min_y is None or y0 < min_y:
                    min_y = y0
                canvas.rectangle([(x0, y0), (x1, y1)], outline=color)

            if len(rects):
                crop[0] = min(textblock.x, crop[0])
                crop[1] = min(textblock.y, crop[1])
                crop[2] = max(textblock.x + textblock.w, crop[2])
                crop[3] = max(textblock.y + textblock.h, crop[3])
        self.cropped_coords = (crop[0] * scale_x, crop[1] * scale_y, crop[2] * scale_x, crop[3] * scale_y)

        if crop:
            self.crop(im, self.cropped_coords)

        canvas.rectangle([(self.cropped_coords[0] - padding, self.cropped_coords[1] - padding), (self.cropped_coords[2] + padding, self.cropped_coords[3] + padding)], outline=(0, 0, 255))
        self.scroll_coords = (min_x, min_y)
        return im

    def crop(self, im = None, coords = None):
        if coords is None:
            coords = self.cropped_coords

        if im is None:
            im = self.image.copy()

        if coords is None:
            return im

        return im.crop(coords)


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
        logger.debug("Export: is_ready? wanted %d vs. %d gotten" % (len(self.files), len(self.result)))
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
