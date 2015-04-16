import urllib2


class HttpMixin(object):
    def http_request(self, url, method, data="", headers=None):
        if url[0:7].lower() != "http://":
            url = "http://%s" % url

        if hasattr(self, 'logger') and self.logger is not None:
            self.logger.debug("Sending http request. Url: %s, Data: %s, Headers: %s" % (url, str(data), str(headers)))

        req = urllib2.Request(url, data, headers)
        req.get_method = lambda: method
        return urllib2.urlopen(req)