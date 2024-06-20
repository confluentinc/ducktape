# Copyright 2015 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from urllib.request import build_opener, Request


class HttpMixin(object):
    def http_request(self, url, method, data="", headers=None, timeout=None):
        if url[0:7].lower() != "http://":
            url = "http://%s" % url

        if hasattr(self, 'logger') and self.logger is not None:
            self.logger.debug("Sending http request. Url: %s, Data: %s, Headers: %s" % (url, str(data), str(headers)))

        req = Request(url, data, headers)
        req.get_method = lambda: method
        # The timeout parameter in urllib2.urlopen has strange behavior, and
        # seems to raise errors when set to a number. Using an opener works however.
        opener = build_opener()
        if timeout is None:
            response = opener.open(req)
        else:
            response = opener.open(req, timeout=timeout)

        return response
