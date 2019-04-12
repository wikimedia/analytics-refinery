#!/usr/bin/env python
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
Wikimedia Anaytics Refinery python Druid utility functions.

See util.py, hive.py, hdfs.py in the same folder
"""

import logging
import json

# Ugly but need python3 support
try:
    from urlparse import urlparse
    from urllib2 import (build_opener, Request, HTTPHandler, HTTPError,
                         URLError, urlopen)
except ImportError:
    from urllib.parse import urlparse
    from urllib.request import build_opener, Request, HTTPHandler, urlopen
    from urllib.error import HTTPError, URLError


logger = logging.getLogger('druid-util')


class Druid(object):
    def __init__(self, druid_host):
        self.host = druid_host
        self.coordinator_url = 'http://' + self.host + '/druid/coordinator/v1'

    def list_intervals(self, datasource):
        """
        Gets a list of the intervals that a druid datasource is composed of,
        sorted descending chronologically. The intervals are in the following
        format:
        yyyy-MM-ddThh:mm:ss.fffZ/yyyy-MM-ddThh:mm:ss.fffZ
        """
        url = (self.coordinator_url + '/datasources/' + datasource + '/intervals')
        intervals = json.loads(self.get(url))
        return intervals

    def remove_interval(self, datasource, interval, dry_run=False):
        """
        Sends a DELETE request to the druid coordinator for the selected
        datasource and interval.
        """
        url = (self.coordinator_url + '/datasources/' + datasource + '?kill=true&interval=' + interval)
        self.delete(url, dry_run=dry_run)

    def remove_datasource(self, datasource, deep=False, dry_run=False):
        """
        If deep is false, just sends a DELETE request to the druid coordinator
        for the selected datasource, therefore just disabling it but not deleting
        it from deep storage. Returns the response body of the request if
        it was successful.
        If deep is true, removes each interval in the datasource, effectively deleting
        it from deep storage in hdfs.
        """
        url = (self.coordinator_url + '/datasources/' + datasource)
        self.delete(url, dry_run=dry_run)
        # Removing the datasource from deep storage implies sending a kill signal
        # to each of the segments of the datasource.
        if deep:
            for interval in self.list_intervals(datasource):
                logger.info('Deleting interval {}...'.format(interval))
                self.remove_interval(datasource, interval, dry_run)

    def list_datasources(self):
        url = self.coordinator_url + '/metadata/datasources'
        datasources = json.loads(self.get(url))
        return datasources

    def delete(self, url, dry_run=False):
        opener = build_opener(HTTPHandler)
        request = Request(url)
        request.get_method = lambda: 'DELETE'
        if dry_run:
            print('DELETE ' + url)
            return '{}'
        else:
            try:
                return opener.open(request).read()
            except HTTPError as e:
                logger.error('HTTPError = ' + str(e.code))
            except URLError as e:
                logger.error('URLError = ' + str(e.reason))
            except Exception:
                import traceback
                logger.error('generic exception: ' + traceback.format_exc())

    def get(self, url):
        try:
            return urlopen(url).read().decode('utf-8')
        except HTTPError as e:
            logger.error('HTTPError = ' + str(e.code))
        except URLError as e:
            logger.error('URLError = ' + str(e.reason))
        except Exception:
            import traceback
            logger.error('generic exception: ' + traceback.format_exc())
