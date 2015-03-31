import urllib2
from ducktape.logger import Logger


class HttpMixin(Logger):

    def http_request(self, url, method, data="", headers=None):
        if url[0:7].lower() != "http://":
            url = "http://%s" % url

        self.logger.debug("Sending http request. Url: %s, Data: %s, Headers: %s" % (url, str(data), str(headers)))
        req = urllib2.Request(url, data, headers)
        req.get_method = lambda: method
        return urllib2.urlopen(req)