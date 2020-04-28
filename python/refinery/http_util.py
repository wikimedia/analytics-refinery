#!/usr/bin/env python3
# -*- coding: utf-8 -*-

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

"""
Wikimedia Analytics Refinery python HTTP utility functions.

See util.py, hive.py, hdfs.py in the same folder
"""

import logging

from urllib.request import build_opener, Request, HTTPHandler, urlopen


logger = logging.getLogger('http-util')


class HTTP(object):

    def _http_call(self, url, method, data, headers):
        opener = build_opener(HTTPHandler)
        request = Request(url, data, headers=headers)
        request.get_method = lambda: method
        logger.debug(method + ' HTTP request to ' + url)
        return opener.open(request).read().decode('utf-8')

    def get(self, url, headers={}):
        return self._http_call(url, 'GET', None, headers)

    def post(self, url, data, headers={}):
        return self._http_call(url, 'POST', data, headers)

    def put(self, url, data=None, headers={}):
        return self._http_call(url, 'PUT', data, headers)

    def delete(self, url, data=None, headers={}):
        return self._http_call(url, 'DELETE', data, headers)
