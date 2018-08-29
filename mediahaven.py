# Usage:
# mm = MediaHaven([config])
# By default it will read config.ini from the current working directory and
# use that config.
# You can also use a dict with keys:
#   - rest_connection_url
#   - oai_connection_url


import requests
from requests.exceptions import ReadTimeout
import logging
import http.client as http_client
from urllib.parse import quote_plus, urlparse
import time
from . import alto
from .config import Config
from . import decorators
from .cache import LocalCacher
from PIL import Image, ImageDraw
from io import BytesIO
from .oai import OAI
from functools import partial

logger = logging.getLogger(__name__)


def _remove_user_pass_from_url(url):
    if type(url) is str:
        url = urlparse(url)
    return url._replace(netloc="{}:{}".format(url.hostname, url.port)).geturl()


class MediaHavenException(Exception):
    pass


class MediaHavenTimeoutException(MediaHavenException):
    pass


class MediaHavenRequest:
    """
    Automagically replaces the ReadTime Exception with MediaHavenTimeoutException
    """
    exception_wrapper = decorators.exception_redirect(MediaHavenTimeoutException, ReadTimeout, logger)

    def __getattr__(self, item):
        func = getattr(requests, item)
        # quick hack to always use proxy:
        # func = functools.partial(func, proxies={'http': 'proxy:80', 'https': 'proxy:80'})
        return MediaHavenRequest.exception_wrapper(func)


req = MediaHavenRequest()


class MediaHaven:
    def __init__(self, config=None):
        self.config = Config(config, 'mediahaven')
        _ = self.config
        self.url = urlparse(_['rest_connection_url'])
        self.token = None
        self.tokenText = None
        self.timeout = None

        if not self.config.is_false('timeout'):
            self.timeout = int(_['timeout'])

        self.__cache = LocalCacher(500)
        if 'cache' in _:
            self.__cache = _['cache']

        if not self.config.is_false('debug'):
            # logging.basicConfig()
            logger.setLevel(logging.DEBUG)
            # logger.propagate = True
            logger.debug('Debugging enabled through configuration')

    def get_cacher(self):
        return self.__cache

    def set_cacher(self, cache):
        self.__cache = cache
        return self

    def oai(self):
        _ = self.config
        url = urlparse(_['oai_connection_url'])
        return OAI(_remove_user_pass_from_url(url), auth=(url.username, url.password))

    def refresh_token(self):
        """Fetch a new token based on the user/pass combination of config
        """
        logger.info('Refreshing oauth access token (username %s)', self.url.username)
        r = req.post('%s%s' % (_remove_user_pass_from_url(self.url), '/resources/oauth/access_token'),
                     auth=(self.url.username, self.url.password),
                     data={'grant_type': 'password'},
                     timeout=self.timeout)
        self._validate_response(r)
        self.token = r.json()
        self.tokenText = self.token['token_type'] + ' ' + self.token['access_token']
        return True

    @staticmethod
    def _validate_response(r):
        if r.status_code < 200 or r.status_code >= 300:
            logger.warning("Wrong status code %d: %s ", r.status_code, r.text)
            raise MediaHavenException("Wrong status code %d: %s " % (r.status_code, r.text))

    def call_absolute(self, url, params=None, method=None, raw_response=False):
        if method is None:
            method = 'get'

        def do_call():
            if not self.tokenText:
                self.refresh_token()
            res = getattr(req, method)(url, headers={'Authorization': self.tokenText},
                                       timeout=self.timeout, params=params)
            logger.debug("HTTP %s %s with params %s returns status code %d", method, url, params, res.status_code)
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
        return self.call_absolute(_remove_user_pass_from_url(self.url) + url, *args, **kwargs)

    @decorators.classcache
    def one(self, q=None, **kwargs):
        """Execute a mediahaven search query, return first result (or None)
        """
        params = {
            "startIndex": 0,
            "nrOfResults": 1
        }
        for k in kwargs:
            params[k] = kwargs[k]
        if q is not None:
            params['q'] = q
        res = self.call('/resources/media/', params)
        if not res:
            return None
        if res['totalNrOfResults'] == 0:
            logger.debug('No results found for params %s', params)
            return None
        return res['mediaDataList'][0]

    def search(self, q, start_index=0, nr_of_results=25):
        """Execute a mediahaven search query
        """
        return SearchResultIterator(self, q, start_index, nr_of_results)

    @staticmethod
    def set_log_http_requests(enabled=False):
        """Toggle logging of http requests
        """
        http_client.HTTPConnection.debuglevel = 1 if enabled else 0
        log_level = logging.DEBUG if enabled else logging.WARNING

        # logging.basicConfig()
        logging.getLogger().setLevel(log_level)
        requests_log = logging.getLogger("requests.packages.urllib3")
        requests_log.setLevel(log_level)
        # requests_log.propagate = enabled

    def media(self, media_object_id, action=None, *args, **kwargs):
        """Do a media query
        """
        url = '/resources/media/%s' % quote_plus(media_object_id)
        if action:
            url = '%s/%s' % (url, action)
        if action in ['fragments']:
            return MediaDataListIterator(self, url=url)
        return self.call(url, *args, **kwargs)

    def export(self, media_object_id, reason=None):
        """Export a file
        """
        params = {"exportReason": reason} if reason else None
        res = self.media(media_object_id, 'export', method='post', raw_response=True, params=params)
        return Export(self, res.headers['Location'], res.json())

    @staticmethod
    def get_alto_location(media_object_id, rights_owner):
        return 'http://archief-media.viaa.be/viaa/MOB/%s/%s/%s.xml' % \
               (rights_owner.replace(' ', '').upper(), media_object_id, media_object_id)

    def get_alto_location_oai(self, fragment_id):
        metadata = self.oai().GetRecord(identifier='umid:%s' % fragment_id, metadataPrefix='mets').metadata
        return metadata['mets']['fileSec']['fileGrp']['file'][0]['FLocat']['@href']

    def get_alto_location_export(self, media_object_id, max_timeout=None):
        export = self.export(media_object_id)
        if max_timeout is None:
            max_timeout = 15
        repeats = max_timeout * 10
        logger.debug("Get export for %s", media_object_id)
        while repeats > 0 and not export.is_ready():
            repeats -= 1
            time.sleep(0.1)

        if not export.is_ready():
            msg = "Timeout of %ds reached without export being ready for '%s'" % (max_timeout, export.location)
            logger.warning(msg)
            raise MediaHavenException(msg)

        files = export.get_files()
        if files is None or len(files) != 1:
            logger.warning("Couldn't get files. Length: %s", 'None' if files is None else str(len(files)))
            raise MediaHavenException("Couldn't get files...")

        return files[0]

    @decorators.classcache
    def get_alto(self, pid, max_timeout=None):
        cp = [val for val in self.one('+(externalId:%s)' % pid)['mdProperties'] if val['attribute'] == 'CP'][0]['value']
        res = self.search('+(originalFileName:%s_alto.xml)' % pid)

        if len(res) == 0:
            logger.warning('Expected 1 result for %s, got none', pid)
            return None

        if len(res) > 1:
            logger.warning('Expected 1 result for %s, gotten %d', pid, len(res))
            raise MediaHavenException("Expected only one result")

        res = next(res)

        attempt = 0
        attempts = [
            lambda: self.get_alto_location(res['mediaObjectId'], cp),
            lambda: self.get_alto_location_oai(res['fragmentId']),
            lambda: self.get_alto_location_export(res['mediaObjectId'], max_timeout=max_timeout)
        ]

        while attempt <= len(attempts):
            try:
                attempt += 1
                file = attempts[attempt-1]()
                result = req.get(file)
                if result.status_code != requests.codes.ok:
                    msg = 'Attempt #%d: incorrect status code %d for pid "%s", export url: %s' % \
                          (attempt, result.status_code, pid, file)
                    raise MediaHavenException(msg)
                logger.debug('Gotten alto with attempt #%d: %s' % (attempt, file))
                return alto.AltoRoot(result.content)
            except MediaHavenException as e:
                logger.warning(e)
            except Exception as e:
                logger.exception(e)
        raise MediaHavenException("Could not load alto file after %d attempts" % attempt)

    def fragments(self, media_object_id):
        """Get fragments for a media object
        """
        return self.media(media_object_id, 'fragments')

    def get_preview(self, pid):
        """Get a preview of an item (fetches previewImagePath for pid)
        """
        return PreviewImage(pid, self)


class PreviewImage:
    def __init__(self, pid, mh):
        self.mh = mh
        self.pid = pid
        self.meta = None
        self.image = None
        self.closed = True
        # self.__enter__()

    def __enter__(self):
        return self.open()

    def __exit__(self, *args, **kwargs):
        return self.close()

    def open(self):
        if not self.closed:
            raise IOError("File already open")

        item = self.mh.one('+(externalId:%s)' % self.pid)
        self.closed = False
        if len(item) == 0:
            return None
        self.meta = item
        self.image = Image.open(BytesIO(req.get(self.meta['previewImagePath']).content))
        return self

    def close(self):
        if self.closed:
            raise IOError("Cannot close a not-open file")
        if self.image:
            self.image.close()
        self.closed = True
        return self

    def highlight_confidence(self, im=None):
        if self.closed:
            raise IOError("Cannot work on a closed file")

        pages = list(self.get_alto().pages())
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
        logger.debug('page: %dx%d, image: %dx%d, scale: %1.2fx%1.2f', w, h, im.size[0], im.size[1], scale_x, scale_y)

        for rect in p.words():
            if rect.x is None:
                logger.warning("Rect with missing x: %s", rect.__dict__)
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

    # @memoize
    # @decorators.log_call(logger=logger, result=True)
    def get_words(self, words, search_kind=None):
        if type(words) is str:
            words = [words]
        if type(words[0]) is str:
            words = [words]

        if search_kind is None:
            search_kind = 'containsproximity'
        page = list(self.get_alto().pages())
        if len(page) != 1:
            # logger.warning("Expected only 1 page, got %d", len(page))
            raise MediaHavenException("Expected only 1 page, got %d" % len(page))
        page = page[0]
        textblocks_extent = None
        words_extent = None

        results = []
        textblocks = []
        for textblock in page.textblocks():
            rects = []
            textblock_extent = Extent.from_object(textblock)
            textblocks.append(textblock_extent)

            rects.extend(SearchKinds.multi_run(search_kind, list(textblock.words()), words))

            for rect in rects:
                word_extent = Extent.from_object(rect)
                results.append({
                    "extent": word_extent,
                    "word": rect,
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

    def highlight_words(self, words, search_kind=None, im=None, crop=True, highlight_textblocks_color=None,
                        words_color=(255, 0, 0)):
        if im is None:
            im = self.image.copy()
        canvas = ImageDraw.Draw(im)
        coords = self.get_words(words, search_kind=search_kind)

        (page_w, page_h) = coords['page_dimensions']
        (w, h) = im.size
        scale_x = w / page_w
        scale_y = h / page_h

        logger.debug('Scaling, page: %dx%d vs img %dx%d => %3.2fx%3.2f', page_w, page_h, w, h, scale_x, scale_y)

        for word in coords['words']:
            rect = word['extent'].scale(scale_x, scale_y)
            canvas.rectangle(rect.as_coords(), outline=words_color)

        if highlight_textblocks_color is not None:
            for textblock_extent in coords['textblocks']:
                canvas.rectangle(textblock_extent.scale(scale_x, scale_y).as_coords(),
                                 outline=highlight_textblocks_color)

        if coords['extent_textblocks']:
            if crop:
                im = im.crop(coords['extent_textblocks'].scale(scale_x, scale_y).as_box())
            elif highlight_textblocks_color is not None:
                canvas.rectangle(coords['extent_textblocks'].scale(scale_x, scale_y).as_coords(),
                                 outline=highlight_textblocks_color)

        pagepad = 30
        # page_w = w
        # page_h = h
        page_w *= scale_x
        page_h *= scale_y
        page_w -= 2*pagepad
        page_h -= 2*pagepad

        # highlight page coords
        # pagecoords = [(pagepad, pagepad), (page_w, page_h)]
        # canvas.rectangle(pagecoords, outline=(0, 255, 0))
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
            if res.status_code != requests.codes.ok:
                raise MediaHavenException("Invalid status code %d" % res.status_code)
            data.append(res.content)
        return data


class MediaDataListIterator:
    def __init__(self, mh, params=None, url='/resources/media', start_index=0, buffer_size=25, param_map=None):
        self.buffer_size = buffer_size
        self.mh = mh
        self.params = params if params is not None else dict()
        self.url = url
        self.length = None
        self.buffer = []
        self.i = start_index
        self.buffer_idx = 0
        if param_map is not None:
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

        if self.length is not None and self.length != results['totalNrOfResults']:
            raise MediaHavenException(
                "Difference in length, had %d, now getting %d" % (self.length, results['totalNrOfResults']))

        self.length = results['totalNrOfResults']
        self.buffer = results['mediaDataList']

    def __len__(self):
        if self.length is None:
            self.fetch_next()
        return self.length

    def __next__(self):
        if self.length is None:
            self.fetch_next()

        if self.i >= self.length:
            raise StopIteration()

        self.i += 1
        self.buffer_idx += 1

        if self.buffer_idx >= len(self.buffer) and self.i < self.length:
            self.buffer_idx = 0
            self.fetch_next()

        return self.buffer[self.buffer_idx - 1]

    def set_buffer_size(self, buffer_size):
        self.buffer_size = buffer_size


class SearchResultIterator(MediaDataListIterator):
    def __init__(self, mh, q, start_index=0, buffer_size=25):
        super().__init__(mh, params={"q": q}, start_index=start_index, buffer_size=buffer_size)


def set_obj_key(key, value, obj):
    setattr(obj, key, value)
    return obj


class SearchKinds:
    @staticmethod
    def multi_run(kind, words, list_of_tocheck, **kwargs):
        result = []
        list_of_tocheck = [x for x in set(tuple(x) for x in list_of_tocheck)]
        # logger.debug('to check: %s', list_of_tocheck)
        for tocheck in list_of_tocheck:
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
