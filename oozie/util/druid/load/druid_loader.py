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

import sys
import argparse
import logging
import requests
import datetime
import time


logger = logging.getLogger(__name__)


class DruidLoader(object):

    _DRUID_DATASOURCE_FIELD = '*DRUID_DATASOURCE*'
    _INTERVALS_ARRAY_FIELD = '*INTERVALS_ARRAY*'
    _INPUT_PATH_FIELD = '*INPUT_PATH*'
    _HADOOP_QUEUE_FIELD = '*HADOOP_QUEUE*'

    _LAUNCH_TASK_PATH = '/druid/indexer/v1/task'
    _CHECK_TASK_PATH = '/druid/indexer/v1/task/{0}/status'

    _STATUS_RUNNING = 'RUNNING'
    _STATUS_FAILED = 'FAILED'
    _STATUS_SUCCEEDED = 'SUCCEEDED'

    def __init__(self, template_path, target_datasource, data_path, period,
                 host='http://an-druid1001.eqiad.wmnet:8090',
                 hadoop_queue='default',
                 sleep=10):
        self.template_path = template_path
        self.target_datasource = target_datasource
        self.data_path = data_path
        self.period = period
        self.host = host
        self.hadoop_queue = hadoop_queue
        self.sleep = sleep
        self._init_json()

    def _init_json(self):
        template = open(self.template_path, 'r').read()
        intervals_array = '["{0}"]'.format(self.period)
        self.json = (template.
                     replace(self._DRUID_DATASOURCE_FIELD,
                             self.target_datasource).
                     replace(self._INPUT_PATH_FIELD, self.data_path).
                     replace(self._INTERVALS_ARRAY_FIELD, intervals_array).
                     replace(self._HADOOP_QUEUE_FIELD, self.hadoop_queue))
        logger.debug('Json to be sent:\n{0}'.format(self.json))

    def _start(self):
        url = self.host + self._LAUNCH_TASK_PATH
        headers = {'Content-type': 'application/json'}
        req = requests.post(url, data=self.json, headers=headers)
        if req.status_code == requests.codes.ok:
            self.task_id = req.json()['task']
            logger.debug('Indexation launched using url {0}'.format(url))
        else:
            logger.error(req.text)
            raise RuntimeError('Druid indexation start returned bad status')

    def _update_status(self):
        url = self.host + self._CHECK_TASK_PATH.format(self.task_id)
        req = requests.get(url)
        if req.status_code == requests.codes.ok:
            self.current_status = req.json()['status']['status'].strip()
            logger.debug('Indexation status update to {0}'.format(
                self.current_status))
        else:
            logger.error(req.text)
            raise RuntimeError('Druid indexation check returned bad status ' +
                               'for task Id {0}'.format(self.task_id))

    def execute(self):
        try:
            self._start()
            self._update_status()
            while self.current_status == self._STATUS_RUNNING:
                time.sleep(self.sleep)
                self._update_status()
            if self.current_status == self._STATUS_SUCCEEDED:
                return 0
            if self.current_status == self._STATUS_FAILED:
                return 1
        except Exception as e:
            logger.error("An error occured:" + str(e))
            return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Launch a synchronous druid indexation for a day.')

    parser.add_argument('template',
                        help='The druid indexation json template path')
    parser.add_argument('target', help='The druid datasource to index')
    parser.add_argument('data', help='The druid indexation json data path')
    parser.add_argument('period', help='The druid indexation period ' +
                        '(YYYY-MM-DD/YYY-MM-DD) format')
    parser.add_argument('--overlord',
                        default='http://an-druid1001.eqiad.wmnet:8090',
                        help='The druid overlord url (defaults to ' +
                             'http://an-druid1001.eqiad.wmnet:8090)')
    parser.add_argument('--hadoop-queue',
                        default='default',
                        help='The hadoop queue for jobs to run in ' +
                        '(defaults to default')
    parser.add_argument('--silent', action='store_true',
                        help='Log only warning and error messages')

    args = parser.parse_args()

    logging.basicConfig(level=(
        logging.WARNING if args.silent else logging.DEBUG))
    loader = DruidLoader(args.template, args.target,
                         args.data, args.period, host=args.overlord,
                         hadoop_queue=args.hadoop_queue)
    return_code = loader.execute()
    sys.exit(return_code)
