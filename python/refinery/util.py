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
Wikimedia Anaytics Refinery python utility functions.

See hive.py, hdfs.py, druid.py in the same folder
"""

import dns.resolver
import logging
import os
import subprocess
import glob


logger = logging.getLogger('refinery-util')
MW_CONFIG_PATH = '/srv/mediawiki-config'

CLOUD_DB_HOST = 'labsdb1012.eqiad.wmnet'
CLOUD_DB_POSTFIX = '_p'
JDBC_TEMPLATE = 'jdbc:mysql://{host}/{dbname}'
JDBC_TEMPLATE_WITH_PORT = 'jdbc:mysql://{host}:{port}/{dbname}'


def is_yarn_application_running(job_name):
    '''
    Returns true if job_name is found in the output
    of yarn application -list and has a status of
    RUNNING or ACCEPTED.  Returns false otherwise.

    Note:  Error checking on the yarn shell command is not good.
    This command will return false on any command failure.
    '''
    command = '/usr/bin/yarn application -list 2>/dev/null | ' + \
        'grep -qP "(^|\s){}(?=\s|$).*(RUNNING|ACCEPTED)"'.format(job_name)
    logging.debug('Running: {0}'.format(command))
    retval = os.system(command)
    return retval == 0


def yarn_application_id(job_name):
    """
    Returns the YARN applicationId for the given job_name.  If there is no
    currently running or scheduled job with the job_name, returns None.
    """
    command = '/usr/bin/yarn application -list 2>/dev/null | grep \'{}\''.format(job_name)
    output = sh(command, check_return_code=False)

    # if no output returned, then there is no job currently running with this job_name.
    if len(output) == 0:
        return None
    # Else return the first element in the output, which should be the applicationId.
    else:
        return output.split('\t')[0]


def yarn_application_status(application_id):
    """
    Returns the output of yarn application -status for the given application_id.
    If there is no running application with this applicaiton_id, returns
    a custom message stating so.
    """
    command = '/usr/bin/yarn application -status {}'.format(application_id)
    output = sh(command, check_return_code=False)

    if len(output) == 0:
        output = 'No job with applicationId {} is currently scheduled'.format(application_id)

    return output


def sh(command, check_return_code=True, strip_output=True, return_stderr=False):
    """
    Executes a shell command and return the stdout.

    Parameters
        command             : The command to run.  Either an array or a string.
                              If it is a string, shell=True will be passed to
                              subprocess.Popen.
        check_return_code   : If the command does not exit with 0, a RuntimeError
                              will be raised.
        strip_output        : If True, output.strip() will be called before returning.
                              Default: True
        return_stderr       : If True, (stdout, stderr) will be returned as a tuple.
                              Default: False

    Returns
        The stdout output of the shell command.

    Raises
        RuntimeError         : check_return_code == True and the command exited
                               with a non zero exit code.
    """

    # command_string is just for log messages.
    if isinstance(command, list):
        shell = False
        command_string = ' '.join(command)
    else:
        shell = True
        command_string = command

    logger.debug('Running: {0}'.format(command_string))
    p = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=shell)
    stdout, stderr = p.communicate()
    if check_return_code and p.returncode != 0:
        raise RuntimeError("Command: {0} failed with error code: {1}"
                           .format(command_string, p.returncode), stdout, stderr)
    if strip_output:
        stdout = stdout.strip()
        stderr = stderr.strip()

    if return_stderr:
        return (stdout, stderr)
    else:
        return stdout


def get_mediawiki_section_dbname_mapping(mw_config_path=MW_CONFIG_PATH, use_x1=False):
    db_mapping = {}
    if use_x1:
        dblist_section_paths = [mw_config_path.rstrip('/') + '/dblists/all.dblist']
    else:
        dblist_section_paths = glob.glob(mw_config_path.rstrip('/') + '/dblists/s[0-9]*.dblist')
    for dblist_section_path in dblist_section_paths:
        with open(dblist_section_path, 'r') as f:
            for db in f.readlines():
                db_mapping[db.strip()] = dblist_section_path.strip().rstrip('.dblist').split('/')[-1]

    return db_mapping


def get_dbstore_host_port(use_x1, dbname, db_mapping=None,
                          mw_config_path=MW_CONFIG_PATH):
    if not db_mapping:
        db_mapping = get_mediawiki_section_dbname_mapping(mw_config_path, use_x1)
    if not db_mapping:
        raise RuntimeError("No database mapping found at {}. Have you configured correctly the mediawiki-config path?"
                           .format(mw_config_path))
    if dbname == 'staging':
        shard = 'staging'
    elif dbname == 'centralauth':
        # The 'centralauth' db is a special case, not currently
        # listed among the mediawiki-config's dblists. The more automated
        # solution would be to parse db-eqiad.php in mediawiki-config, but it
        # would add more complexity than what's necessary.
        shard = 's7'
    elif use_x1:
        shard = 'x1'
    else:
        try:
            shard = db_mapping[dbname]
        except KeyError:
            message = (
                "The database {} is not listed among the dblist files of the supported sections." +
                " Perhaps try --use-x1 if your database is on the x1 cluster (eg. centralauth)"
            ).format(dbname)
            raise RuntimeError(message)
    answers = dns.resolver.query('_' + shard + '-analytics._tcp.eqiad.wmnet', 'SRV')
    host, port = str(answers[0].target).strip('.'), str(answers[0].port)
    return (host, port)


def get_jdbc_string(dbname, labsdb, db_mapping=None):
    """
    Params
        dbname      the database name, like enwiki, etwiki, etc
        labsdb      True: use the cloud cluster, False: use production replica cluster
        db_mapping  (None) if not specified, fetch this from mediawiki-config
    """

    # for labsdb, hostname must be specified, we append db_postfix to the dbname
    if labsdb:
        return JDBC_TEMPLATE.format(host=CLOUD_DB_HOST, dbname=dbname + CLOUD_DB_POSTFIX)
    # for production replicas, we have to query for the hostname and port
    else:
        (host, port) = get_dbstore_host_port(False, dbname, db_mapping)
        return JDBC_TEMPLATE_WITH_PORT.format(host=host, port=port, dbname=dbname)
