# Usage:
# mm = MediaHaven([config])
# By default it will read config.json from the current working directory and
# use that config.
# You can also use a dict with keys:
#    - user
#    - pass
#    - host (eg. archief-qas.viaa.be)
#    - port (eg. 443)
#    - basePath (eg. /mediahaven-rest-api)
#    - protocol (eg. 'https')


import requests as req
import pandas as pd
import logging
import http.client as http_client

class MediaHavenException(Exception):
    pass

class MediaHaven:
    def __init__(self, config = None):
        if config == None:
            config = 'config.json'
        if type(config) == str:
            try:
               config = pd.read_json(config)
            except ValueError:
               config = pd.read_json(config, typ='series')
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

    def call(self, url, params = {}):
        """Execute a call to MediaHaven server
        """
        def do_call():
            if not self.tokenText:
                self.refresh_token()
            return req.get(self.URL + url, headers={'Authorization': self.tokenText}, params = params)

        r = do_call()
        if r.status_code < 200 or r.status_code >= 300:
            self.refresh_token()
            r = do_call()
        self._validate_response(r)
        return r.json()
    
    def search(self, q, startIndex = 0, nrOfResults = 25):
        """Execute a mediahaven search query
        """
        return self.call('/resources/media', {"q": q, "startIndex": startIndex, "nrOfResults": nrOfResults})
    
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


