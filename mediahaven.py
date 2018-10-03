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
import urllib3
from urllib.parse import quote_plus, urlparse, ParseResult
import time
from . import alto
from .config import Config
from . import decorators
from .cache import DummyCacher
from PIL import Image, ImageDraw
from io import BytesIO
from .oai import OAI
from collections.abc import Mapping
from functools import partial
from time import sleep
from datetime import datetime
import email.utils as rfc5322

logger = logging.getLogger(__name__)


def _remove_auth_from_url(url):
    """
    Obfuscate the auth from a URL
    :param url: str|ParseResult
    :return: string
    """
    if type(url) is not ParseResult:
        url = urlparse(url)
    replaced = url._replace(netloc=url.hostname)
    return replaced.geturl()


class MediaHavenException(Exception):
    pass


class MediaHavenTimeoutException(MediaHavenException):
    pass


def too_many_req_decorator(sleeptime=1):
    def _decorator(func):
        def new_func(*args, **kwargs):
            r = func(*args, **kwargs)
            attempts = 5
            while attempts > 0 and r is not None and r.status_code == 429:
                attempts -= 1

                logger.info('Too many req, sleeping for %d secs and retrying another %d times', sleeptime, attempts)

                if 'retry-after' in r.headers:
                    logger.info('App suggested retrying after "%s"', r.headers['retry-after'])
                    try:
                        secs = int(r.headers['retry-after'])
                    except ValueError:
                        # parse HTTP-date, MUST also support RFC 850 and ANSI C asctime (but f that for the moment!)
                        # rfc5322 date-time
                        secs = rfc5322.parsedate_to_datetime(r.headers['retry-after'])
                        secs = secs - datetime.now()
                        secs = secs.total_seconds()

                    if secs <= 0:
                        secs = None

                    logger.info('App suggested sleeping for %s secs', str(secs))
                    # todo: replace sleeptime with suggested retry-after value? (3600 seems default, we really wanna wait an hour?)

                sleep(sleeptime)
                r = func(*args, **kwargs)
            return r
        return new_func
    return _decorator


class MediaHavenRequest:
    """
    Automagically replaces the ReadTimeout Exception with MediaHavenTimeoutException
    """
    exception_wrapper = decorators.exception_redirect(MediaHavenTimeoutException, ReadTimeout)

    def __init__(self):
        c = Config(section='mediahaven')
        insecure_ssl = 'insecure_ssl' in c and not c.is_false('insecure_ssl')
        self._sleeptime = 1
        if 'sleeptime' in c:
            self._sleeptime = int(c['sleeptime'])
        self._insecure_ssl = insecure_ssl
        if insecure_ssl:
            logger.warning('Using insecure SSL (not verifying certificates)')
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    def __getattr__(self, item):
        func = getattr(requests, item)
        # import functools
        if item in ('get', 'post'):
            if self._insecure_ssl:
                # ignore ssl verification
                func = partial(func, verify=False)

            # quick hack to always use proxy:
            # func = partial(func, proxies={'http': 'proxy:80', 'https': 'proxy:80'})

            # wrap in too many req handler
            func = too_many_req_decorator(self._sleeptime)(func)

        return MediaHavenRequest.exception_wrapper(func)


req = MediaHavenRequest()


class MediaHaven:
    classcacheVersionNumber = 0

    def __init__(self, config=None, **kwargs):
        self.config = Config(config, 'mediahaven')
        _ = self.config
        _.update(kwargs)
        self.url = urlparse(_['rest_connection_url'])
        self.token = None
        self.tokenText = None
        self.timeout = None
        self.buffer_size = None

        if not _.is_false('buffer_size'):
            self.buffer_size = _['buffer_size']

        if not _.is_false('timeout'):
            self.timeout = int(_['timeout'])

        self.__cache = DummyCacher()
        if 'cache' in _:
            self.__cache = _['cache']

        if not self.config.is_false('debug'):
            logger.setLevel(logging.DEBUG)
            logger.debug('Debugging enabled through configuration')

    def get_cacher(self):
        return self.__cache

    def set_cacher(self, cache):
        self.__cache = cache
        return self

    def oai(self):
        _ = self.config
        url = urlparse(_['oai_connection_url'])
        return OAI(_remove_auth_from_url(url), auth=(url.username, url.password))

    def refresh_token(self):
        """Fetch a new token based on the user/pass combination of config
        """
        logger.debug('Refreshing oauth access token (username %s)', self.url.username)
        r = req.post('%s%s' % (_remove_auth_from_url(self.url), '/resources/oauth/access_token'),
                     auth=(self.url.username, self.url.password),
                     data={'grant_type': 'password'},
                     timeout=self.timeout)
        self._validate_response(r)
        self.token = r.json()
        self.tokenText = self.token['token_type'] + ' ' + self.token['access_token']
        return True

    def _validate_response(self, r):
        if r.status_code == 401:
            raise MediaHavenException('User "%s" not authorized, code %d: %s' % (self.url.username, r.status_code, r.text))

        if r.status_code < 200 or r.status_code >= 300:
            # logger.warning("Wrong status code %d: %s ", r.status_code, r.text)
            raise MediaHavenException("Wrong status code %d: %s " % (r.status_code, r.text))

    def call_absolute(self, url, params=None, method=None, raw_response=False):
        if method is None:
            method = 'get'

        def do_call():
            if not self.tokenText:
                self.refresh_token()
            res = getattr(req, method)(url,
                                       headers={'Authorization': self.tokenText},
                                       timeout=self.timeout,
                                       params=params)
            logger.debug("HTTP %s %s with params %s returns status code %d", method, url, params, res.status_code)
            return res

        r = None
        try:
            r = do_call()
        except MediaHavenTimeoutException as e:
            # let exception pass, allow auto-retry on read timeout
            logger.info(e)
            r = do_call()

        if r is None or r.status_code < 200 or r.status_code >= 300:
            self.refresh_token()
            r = do_call()
        self._validate_response(r)
        if raw_response:
            return r
        return r.json()

    def call(self, url, *args, **kwargs):
        """Execute a call to MediaHaven server
        """
        return self.call_absolute(_remove_auth_from_url(self.url) + url, *args, **kwargs)

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
        return MediaObject(res['mediaDataList'][0], q)

    def search(self, q, start_index=0, nr_of_results=None):
        """Execute a mediahaven search query
        """
        if nr_of_results is None:
            nr_of_results = self.buffer_size
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
        try:
            return metadata['mets']['fileSec']['fileGrp']['file'][0]['FLocat']['@href']
        except KeyError:
            return None

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
        if type(pid) is MediaObject:
            data = pid
            pid = pid['externalId']
        else:
            data = self.one('+(externalId:%s)' % pid)
        cp = data['mdProperties']['CP'][0]
        res = self.one('+(originalFileName:%s_alto.xml)' % pid)

        if res is None:
            logger.debug('Expected 1 result for %s, got none', pid)
            return None

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
                if file is not None:
                    # logger.debug('get_alto: Attempt %d: %s', attempt, file)
                    result = req.get(file, timeout=self.timeout)
                else:
                    raise MediaHavenException('Couldnt get URL (result = None)')
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


class MediaObject(Mapping):
    def __init__(self, data, query=None):
        if type(data['mdProperties']) is not MediaObjectMDProperties:
            data['mdProperties'] = MediaObjectMDProperties(data['mdProperties'])
        self.__dict__ = data
        self.__query = query

    def __getitem__(self, k: str):
        return self.__dict__[k]

    def __len__(self) -> int:
        return len(self.__dict__)

    def __iter__(self):
        return self.__dict__.__iter__()

    def keys(self):
        return self.__dict__.keys()

    def values(self):
        return self.__dict__.values()

    def items(self):
        return self.__dict__.items()

    def __str__(self):
        return '%s(%s)' % (type(self).__name__, str({k: str(v) for k, v in self.__dict__.items()}))

    def __repr__(self):
        return '%s(%s)' % (type(self).__name__, repr(self.__query))


class MediaObjectMDProperties(Mapping):
    def __init__(self, props: list):
        self._data = props
        self._keys = set([prop['attribute'] for prop in props])

    def __getitem__(self, k: str) -> list:
        if k not in self._keys:
            raise KeyError('Unknown key "%s"' % k)
        return [prop['value'] for prop in self._data if prop['attribute'] == k]

    def __len__(self) -> int:
        return len(self._keys)

    def __iter__(self):
        return iter(self._keys)

    def keys(self):
        return self._keys

    def values(self):
        return (self._data[k] for k in self._keys)

    def items(self):
        return ((k, self.__getitem__(k)) for k in self._keys)

    def __str__(self):
        return '%s(%s)' % (type(self).__name__, dict(self))


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

    def highlight_words(self, words, search_kind=None, im=None, crop=True, highlight_textblocks_color=None,
                        words_color=(255, 0, 0)):
        if im is None:
            im = self.image.copy()
        canvas = ImageDraw.Draw(im)
        coords = self.get_alto().search_words(words, search_kind=search_kind)

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
    def __init__(self, mh, params=None, url='/resources/media', start_index=0, buffer_size=25, param_map=None, wrapping_class=None):
        self.buffer_size = buffer_size
        self.mh = mh
        self.params = params if params is not None else dict()
        self.url = url
        self.length = None
        self.buffer = []
        self.i = start_index
        self.buffer_idx = 0
        self.wrapping_class = wrapping_class
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

        if self.wrapping_class is None:
            return self.buffer[self.buffer_idx - 1]

        return self.wrapping_class(self.buffer[self.buffer_idx - 1])

    def set_buffer_size(self, buffer_size):
        self.buffer_size = buffer_size


class SearchResultIterator(MediaDataListIterator):
    def __init__(self, mh, q, start_index=0, buffer_size=25):
        wrapping_class = partial(MediaObject, query=q)
        super().__init__(mh, params={"q": q}, start_index=start_index, buffer_size=buffer_size,
                         wrapping_class=wrapping_class)

