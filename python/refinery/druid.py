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
Wikimedia Analytics Refinery python Druid utility functions.

See util.py, hive.py, hdfs.py in the same folder
"""

import logging
import json
import datetime
import time

from refinery.http_util import HTTP
from urllib.parse import urlparse
from urllib.request import build_opener, Request, HTTPHandler, urlopen
from urllib.error import HTTPError, URLError


logger = logging.getLogger('druid-util')

# Reused url patterns
COORDINATOR_URL = '/druid/coordinator/v1'
OVERLORD_URL = '/druid/indexer/v1'

# Task statutes to be monitored
TASK_SUCCESS = 'SUCCESS'
TASK_RUNNING = 'RUNNING'


class Druid(HTTP):
    """
    class providing druid utility functions by extending the HTTP object
    for get, post and delete functions.
    """

    def __init__(self, original_host, coordinator_port=8081,
                 overlord_port=8090):
        self.original_host = original_host
        self.coordinator_port = coordinator_port
        self.overlord_port = overlord_port

        # set coordinator and overlord to their leader hosts
        original_coordinator = 'http://' + self.original_host + ':' + str(self.coordinator_port)
        get_coord_leader_url = original_coordinator + COORDINATOR_URL + '/leader'
        logger.info('Getting druid coordinator leader at ' + get_coord_leader_url)
        leader_coordinator = self.get(get_coord_leader_url)
        self.coordinator_url = leader_coordinator + COORDINATOR_URL

        original_overlord = 'http://' + self.original_host + ':' + str(self.overlord_port)
        get_overlord_leader_url = original_overlord + OVERLORD_URL + '/leader'
        logger.info('Getting druid overlord leader at ' + get_overlord_leader_url)
        leader_overlord = self.get(get_overlord_leader_url)
        self.overlord_url = leader_overlord + OVERLORD_URL

    def list_datasources(self):
        """
        Gets the list of datasources from the coordinator
        """
        datasources_url = self.coordinator_url + '/metadata/datasources'
        logger.info('Listing druid datasources using {}'.format(datasources_url))
        datasources = json.loads(self.get(datasources_url))
        return datasources

    def list_intervals(self, datasource):
        """
        Gets a list of the intervals that a druid datasource is composed of,
        sorted descending chronologically. The intervals are in the following
        format:
        yyyy-MM-ddThh:mm:ss.fffZ/yyyy-MM-ddThh:mm:ss.fffZ
        """
        intervals_url = (self.coordinator_url + '/datasources/'
                         + datasource + '/intervals')
        logger.info('Listing druid intervals for datasource {} using {}'.format(
                     datasource, intervals_url))
        intervals = json.loads(self.get(intervals_url))
        return intervals

    def disable_datasource(self, datasource, dry_run=False):
        """
        Sends a DELETE request to the druid coordinator for the given datasource.
        Note: This doesn't delete segments from deep-storage
        """
        delete_url = (self.coordinator_url + '/datasources/' + datasource)
        logger.info('Disabling druid datasource {} using {}'.format(
                    datasource, delete_url))
        if not dry_run:
            self.delete(delete_url)

    def delete_segments(self, datasource, interval, wait_seconds, dry_run=False):
        """
        Sends a kill-task to the overlord to delete segments belonging
        to the given interval from deep-storage.
        Monitor the kill-task by requesting its status at regular interval,
        waiting for wait_seconds between two consecutive requests.
        Note: If the segments are not disabled (still used by historical-nodes),
              the kill-task will succeed but not data will be deleted
        """
        task_id = 'drop_{0}_{1}'.format(datasource,
            datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S'))
        self._post_kill_task(datasource, interval, task_id, dry_run)

        if dry_run:
            logger.info("Monitoring of kill-task (dry-run, nothing to do)")
        else:
            task_status = TASK_RUNNING
            status_url = self.overlord_url + '/task/' + task_id + '/status'
            while task_status == TASK_RUNNING:
                time.sleep(wait_seconds)
                status_req = json.loads(self.get(status_url))
                task_status = status_req['status']['status'].strip()
                logger.info('Updated kill-task status to {0} ({1})'.format(
                            task_status, task_id))

            if task_status == TASK_SUCCESS:
                logger.info('Kill-task succeeded ({})'.format(task_id))
            elif task_status == TASK_FAILED:
                logger.error('Kill task failed with status {} ({})'.format(
                             task_status, task_id))
                raise Exception('Kill-Task Failed')

    def _post_kill_task(self, datasource, interval, task_id, dry_run=False):
        """
        Posting a kill-task to the druid-overlord
        """
        url = self.overlord_url + '/task'
        payload = '''{{
            "type":"kill",
            "id":"{0}",
            "dataSource":"{1}",
            "interval":"{2}"
        }}'''.format(task_id, datasource, interval)
        logger.info('Kill-task posted to {} with payload {}'.format(url, payload))
        if not dry_run:
            headers = {'Content-type': 'application/json'}
            self.post(url, payload.encode(), headers=headers)

    def delete_datasource(self, datasource, wait_seconds, dry_run=False):
        logger.info('Deleting datasource {}'.format(datasource))
        # Get all segments from datasource to define deletion interval
        # Note: It's important to get intervals BEFORE disabling the datasource
        intervals = self.list_intervals(datasource)
        boundaries = sorted([bound for itv in intervals for bound in itv.split("/")])
        interval_to_delete = '{}/{}'.format(boundaries[0], boundaries[-1])

        # Delete, first disabling then deleting segments from deep-storage
        self.disable_datasource(datasource, dry_run)
        self.delete_segments(datasource, interval_to_delete, wait_seconds, dry_run)
