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

'''
Wikimedia Analytics Refinery python Oozie API utility functions.

Not using https://github.com/developer-sdk/oozie-webservice-api
since it doesn't allow for job-type filtering

See util.py, hive.py, hdfs.py, druid.py in the same folder
'''

import logging
import json
import datetime
import time
import os
import operator

from refinery.http_util import HTTP

from urllib.parse import urlencode
from urllib.request import build_opener, Request, HTTPHandler, urlopen
from urllib.error import HTTPError, URLError


logger = logging.getLogger('oozie-util')


# Possible job-types to filter job-list
JOB_TYPE_WORKFLOW = 'wf'
JOB_TYPE_COORD = 'coordinator'
JOB_TYPE_BUNDLE = 'bundle'

VALID_JOB_TYPES = [JOB_TYPE_WORKFLOW, JOB_TYPE_COORD, JOB_TYPE_BUNDLE]

# JSON sub-objects by jobtype
JSON_JOB_TYPES = {
  JOB_TYPE_WORKFLOW: 'workflows',
  JOB_TYPE_COORD: 'coordinatorjobs',
  JOB_TYPE_BUNDLE: 'bundlejobs'
}

# Possible status to filter job-list
JOB_STATUS_WAITING = 'WAITING'
JOB_STATUS_READY = 'READY'
JOB_STATUS_SUBMITTED = 'SUBMITTED'
JOB_STATUS_RUNNING = 'RUNNING'
JOB_STATUS_SUSPENDED = 'SUSPENDED'
JOB_STATUS_TIMEDOUT = 'TIMEDOUT'
JOB_STATUS_SUCCEEDED = 'SUCCEEDED'
JOB_STATUS_KILLED = 'KILLED'
JOB_STATUS_FAILED = 'FAILED'

VALID_JOB_STATUSES = [ JOB_STATUS_WAITING, JOB_STATUS_READY, JOB_STATUS_SUBMITTED,
                       JOB_STATUS_RUNNING, JOB_STATUS_SUSPENDED, JOB_STATUS_TIMEDOUT,
                       JOB_STATUS_SUCCEEDED, JOB_STATUS_KILLED, JOB_STATUS_FAILED ]

# Possible actions to manage jobs
# Not putting dryrun, rerun and change here as they need to post info
# Detail: We seldom use start as we submit-and-start jobs using run
JOB_ACTION_START = 'start'
JOB_ACTION_SUSPEND = 'suspend'
JOB_ACTION_RESUME = 'resume'
JOB_ACTION_KILL = 'kill'

VALID_JOB_ACTIONS = [JOB_ACTION_START, JOB_ACTION_SUSPEND, JOB_ACTION_RESUME, JOB_ACTION_KILL]


class Oozie(HTTP):
    '''
    class providing Oozie utility functions by extending the HTTP object
    for get, post and delete functions.
    '''

    def __init__(self, url=None):
        self.url = url if url else os.environ['OOZIE_URL']

    def make_datetime(self, oozie_datetime):
        '''
        Parse an Oozie formatted datetime onto a python one
        '''
        return datetime.datetime.strptime(oozie_datetime,
                                          "%a, %d %b %Y %H:%M:%S %Z")

    def list_jobs(self, job_types=[], statuses=[], users=[], names=[], offset=1, len_=50):
        '''
        Get the list of jobs for the given parameters.
        Default empty-list for filters mean no filtering. Accepted values are
        lists of strings.
        Default values for offset and len are oozie default ones. Accepted values
        are positive intergers
        '''
        # Prepare URL parameters dictionnary
        filters = ';'.join(['status=' + s.strip() for s in statuses] +
                         ['user=' + u.strip() for u in users] +
                         ['name=' + n.strip() for n in names])
        params = {
            'offset': offset,
            'len': len_,
            'jobtype': job_types,
            'filter': filters
        }
        jobs_url = self.url + '/v2/jobs?' + urlencode(params)
        logger.debug('Listing oozie jobs using {}'.format(jobs_url))
        jobs = json.loads(self.get(jobs_url))
        return jobs

    def get_job_info(self, job_id, offset=0, len_=0):
        '''
        Get job_id information
        offset and len_ are useful for children actions to be in the result when
        getting coordinator or bundle job information
        '''
        params = {
            'offset': offset,
            'len': len_
        }
        job_url = self.url + '/v2/job/' + job_id + '?' + urlencode(params)
        logger.debug('Getting oozie job information using {}'.format(job_url))
        job_info = json.loads(self.get(job_url))
        return job_info

    def manage_job(self, job_id, action):
        '''
        Update job_id with action
        '''
        params = {'action': action }
        manage_url = self.url + '/v2/job/' + job_id + '?' + urlencode(params)
        logger.debug('Changing oozie job status using {}'.format(manage_url))
        self.put(manage_url)

    def coordinator_next_date(self, coord_id, coord_info=None):
        '''
        Find the next date the coordinator needs to execute.
        We do so by looking at children actions in descending index order
        (from now to previous actions in time). The date we're after is the date
        of the last action not being in SUCCEEDED status before the first action
        in SUCCEEDED status.
        If there are children actions but no action is in SUCCEEDED status, we use
        the coordinator first action in time.
        If there are children actions and the most recent one is in SUCCEEDED
        status, we use the coordinator next materialized date.
        If there are no children actions, use coordinator start_time.
        '''

        # get coord number of actions to initialise offset (if not provided)
        coord_info = coord_info if coord_info else self.get_job_info(coord_id)
        coord_num_actions = coord_info['total']
        # Special case if no actions
        if coord_num_actions == 0:
            return self.make_datetime(coord_info['startTime'])

        # Start with offset at last action in time and go backward
        # looking at 100 actions at a time. Stop when a result is found or
        # all actions have been seen
        offset = coord_num_actions
        len_ = 100
        result_date = None
        prev_date = None
        while not result_date and offset > 0:
            offset = max(offset - len_ + 1, 0)
            actions = self.get_job_info(coord_id, offset=offset, len_=len_)['actions']

            # Build a sorted reduced version of actions [(date, succeeded)]
            actions_reduced = []
            for action in actions:
                succeeded = action['status'] == 'SUCCEEDED'
                date = self.make_datetime(action['nominalTime'])
                actions_reduced.append((date, succeeded))
            actions_reduced = sorted(actions_reduced,
                                     key=operator.itemgetter(0),
                                     reverse=True)
            # If no action or every action has succeded
            # set date to next date to materialize
            if not actions_reduced or actions_reduced[0][1]:
                result_date = self.make_datetime(coord_info['nextMaterializedTime'])
            # At least one action has not yet succeeded
            # Set result to the last one not yet succeded or loop to another
            # offset (possible if many waiting actions)
            else:
                for date, succeeded in actions_reduced:
                    if prev_date and succeeded:
                        result_date = prev_date
                        break
                    else:
                        prev_date = date

        # if no result_date has been found
        if not result_date:
            # There was actions but none in SUCCEEDED state
            if prev_date:
                return prev_date
            else:
                raise RuntimeError('Could not determine next action to run ' +
                                   'for coord {0}'.format(coord_id))

        return result_date

    def bundle_next_date(self, bundle_id):
        '''
        Find the bundle next date to be processed based on its coordinators.
        It is the most ancient next-action-date from the bundle coordinators
        '''
        # get smallest next action date from inner coords ids
        bundle_info = self.get_job_info(bundle_id)
        result_date = None
        for coord_info in bundle_info['bundleCoordJobs']:
            coord_id = coord_info['coordJobId']
            coord_next_date = self.coordinator_next_date(coord_id)
            if not result_date or coord_next_date < result_date:
                result_date = coord_next_date
        if not result_date:
            raise RuntimeError('Could not determine next action to run ' +
                               'for bundle {0}'.format(bundle_id))
        return result_date
