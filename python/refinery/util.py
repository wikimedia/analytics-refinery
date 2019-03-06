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
Wikimedia Anaytics Refinery python utilities.
"""

from collections import defaultdict, OrderedDict
from dateutil import parser
import datetime
import dns.resolver
import logging
import os
import subprocess
import re
import tempfile
import json
import glob

# Ugly but need python3 support
try:
    from urlparse import urlparse
    from urllib2 import (build_opener, Request, HTTPHandler, HTTPError,
                         URLError, urlopen)
except ImportError:
    from urllib.parse import urlparse
    from urllib.request import build_opener, Request, HTTPHandler, urlopen
    from urllib.error import HTTPError, URLError

logger = logging.getLogger('refinery-util')
MW_CONFIG_PATH = '/srv/mediawiki-config'

CLOUD_DB_HOST = 'labsdb-analytics.eqiad.wmnet'
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


class HiveUtils(object):
    """
    A convience object for running hive queries via the Hive CLI.
    Most of the methods here work with table and partition DDL.

    Parameters:
        database    : The Hive database name to use
        options     : Other options to be passed directly to the Hive CLI.
    """

    partition_desc_separator = '/'
    partition_spec_separator = ','

    def __init__(self, database='default', options=''):
        self.database   = database
        if options:
            self.options    = options.split()
        else:
            self.options = []

        self.hivecmd = ['hive'] + self.options + ['--service', 'cli', '--database', self.database]
        self.tables  = {}

    def _tables_get(self):
        """Returns a list of tables in the current database"""
        return self.query('SET hive.cli.print.header=false; SHOW TABLES').splitlines()

    def _tables_init(self, force=False):
        """
        Initializes the self.tables dict.

        If self.tables already has entries, self.tables will just be returned.
        If force is True, then self.tables will be cleared and reinitialized.
        """
        if self.tables and not force:
            return self.tables

        if force:
            self.reset()

        for table in self._tables_get():
            self.tables[table] = {}

        return self.tables

    def reset(self):
        """
        Destroy's this object's table info cache.
        Run this if you think your table state has changed,
        and you want subsequent commands to reload data by
        running the appropriate Hive queries again.
        """
        self.tables = {}

    def get_tables(self):
        """Returns the list of tables in the database"""
        self._tables_init()
        return self.tables.keys()

    def table_exists(self, table):
        """Returns true if the table exists in the current database."""
        self._tables_init()

        return table in self.tables.keys()

    def table_schema(self, table):
        """Returns the table's CREATE schema."""
        self._tables_init()

        if 'schema' not in self.tables[table].keys():
            q = 'SET hive.cli.print.header=false; SHOW CREATE TABLE {0};'.format(table)
            self.tables[table]['schema'] = self.query(q)

        return self.tables[table]['schema']

    def table_metadata(self, table):
        """
        Parses the output of DESCRIBE FORMATTED and stores the
        metadata as a dict in self.tables[table]['metadata'].
        """
        self._tables_init()

        if 'metadata' not in self.tables[table].keys():
            self.tables[table]['metadata'] = {}
            q = 'SET hive.cli.print.header=false; DESCRIBE FORMATTED {0};'.format(table)
            for line in self.query(q).splitlines():
                try:
                    key, value = line.split(':', 1)
                    if value:
                        self.tables[table]['metadata'][key.strip()] = value.strip()
                except ValueError:
                    # not at least two elements in line
                    pass

        return self.tables[table]['metadata']

    def table_location(self, table, strip_nameservice=False):
        """Returns the table's base location by looking at the table's CREATE schema."""
        self._tables_init()

        table_location = self.table_metadata(table)['Location']

        if strip_nameservice and table_location.startswith('hdfs://'):
            table_location = urlparse(table_location)[2]

        return table_location

    def partition_specs(self, table):
        """
        Returns a list of partitions for the given Hive table in partition spec format.

        Returns:
            A list of partititon spec strings.
        """
        self._tables_init()

        # Cache results for later.
        # If we don't know the partitions yet, get them now.
        if 'partitions' not in self.tables[table].keys():
            partition_descs = self.query('SET hive.cli.print.header=false; SHOW PARTITIONS {0};'.format(table)).splitlines()
            # Convert the desc format to spec format and return that
            self.tables[table]['partitions'] = [
                self.partition_spec_from_partition_desc(p)
                for p in partition_descs
            ]

        return self.tables[table]['partitions']

    def partitions(self, table):
        """
        Returns a list of HivePartitions for the given Hive table.

        Returns:
            A list of HivePartition dicts
        """

        # Cache results for later.
        # If we don't know the partitions yet, get them now.
        return [HivePartition(p) for p in self.partition_specs(table)]

    def drop_partitions(self, table, partition_specs):
        """
        Runs ALTER TABLE table DROP PARTITION ... for each of the partition_specs.
        """
        if partition_specs:
            q = self.drop_partitions_ddl(table, partition_specs)
            # This query could be large if there are many partiitons to drop.
            # Use a tempfile when dropping partitions.
            return self.query(q, use_tempfile=True)
        else:
            logger.info("Not dropping any partitions for table {0}.  No partition datetimes were given.".format(table))

    def drop_partitions_ddl(self, table, partition_specs):
        """
        Returns a complete hive statement to drop partitions from
        table for the given partition_specs
        """
        partition_specs.sort()
        return '\n'.join(['ALTER TABLE {0} DROP IF EXISTS PARTITION ({1});'.format(table, spec) for spec in partition_specs])

    @staticmethod
    def partition_spec_from_partition_desc(desc):
        """
        Returns a partition spec from a partition description
        output from a 'SHOW PARTITIONS' Hive statement.
        """

        # Loop through each partition, adding quotes around strings.
        spec_parts = []
        for p in desc.split(HiveUtils.partition_desc_separator):
            (key, value) = p.split('=')
            if not value.isdigit():
                value = '\'{0}\''.format(value)
            spec_parts.append('{0}={1}'.format(key, value))

        # Replace partition_desc_separators with partition_spec_separators.
        return HiveUtils.partition_spec_separator.join(spec_parts)

    @staticmethod
    def partition_spec_from_path(path, regex):
        """
        Given an HDFS path and a regex with matching groups named
        after partition keys, this method returns a partition spec
        suitable for use in Hive partition DDL statements.

        Parameters:
            path     : Path to a partition
            regex    : Regular expression that can extract match groups
                       by partition key names from the path string.
                       regex may be a string or a compiled re.

        Returns:
            A partition spec string.

        Example:
            partition_spec_from_path(
                path='/wmf/data/raw/webrequest/webrequest_text/hourly/2014/05/14/23',
                regex=r'/webrequest_(?P<webrequest_source>[^/]+)/hourly/(?P<year>[^/]+)/(?P<month>[^/]+)/(?P<day>[^/]+)/(?P<hour>[^/]+)'
            )
            returns: 'webrequest_source='text',year=2014,month=05,day=14,hour=23
        """
        if isinstance(regex, basestring):
            regex = re.compile(regex)

        group_matches_in_order = [
            g[1] for g in sorted(
                [(index, group) for group, index in regex.groupindex.items()]
            )
        ]
        match = regex.search(path)
        spec_parts = []
        for key in group_matches_in_order:
            # if the match is a number, no need for quotes
            if match.group(key).isdigit():
                value = match.group(key)
            # otherwise, quote it!
            else:
                value = '\'{0}\''.format(match.group(key))
            spec_parts.append('{0}={1}'.format(key, value))

        return HiveUtils.partition_spec_separator.join(spec_parts)

    @staticmethod
    def partition_datetime_from_spec(spec, regex):
        """
        Given a partition spec string, and a regex that names
        match groups by their date names, this returns
        a datetime object representing this partition.

        Parameters:
            spec     : Partition spec string
            regex    : Regular expression that can extract match groups
                       by partition key names from the spec string.
                       regex may be a string or a compiled re.

        Returns:
            datetime object matching this spec's date.

        Example:
            partition_datetime_from_spec(
                spec='webrequest_source=\'text\',year=2014,month=05,day=14,hour=00',
                regex=r'webrequest_source=(?P<webrequest_source>[^/,]+)[/,]year=(?P<year>[^/,]+)[/,]month=(?P<month>[^/,]+)[/,]day=(?P<day>[^/]+)[/,]hour=(?P<hour>[^/,]+)'
            )
            returns: datetime.datetime(2014, 5, 14, 23, 0)
        """
        if isinstance(regex, basestring):
            regex = re.compile(regex)

        match = regex.search(spec)
        return datetime.datetime(
            int(match.groupdict().get('year')),
            int(match.groupdict().get('month', 1)),
            int(match.groupdict().get('day', 1)),
            int(match.groupdict().get('hour', 0))
        )

    @staticmethod
    def partition_datetime_from_path(path, regex, format):
        """
        Given an HDFS path and a regex with the first matching
        group a date string suitable for passing to dateutil.parser.parse,
        this returns a datetime object representing this partition path.

        Parameters:
            path     : Path to a partition
            regex    : Regular expression that can extract date string
                       as match.group(1) that can be parsed with
                       dateutil.parser.parse.
                       regex may be a string or a compiled re.
            format   : match.group(1) from regex will be passed to
                       datetime.datetime.strptime with this provided
                       format.
        Returns:
            datetime object matching this spec's date.

        Example:
            partition_datetime_from_path(
                path='/wmf/data/webrequest/webrequest_source=text/year=2018/month=1/day=9/hour=0"
                regex=r'.*/(year=.+)$',
                format=k'year=%Y/month=%m/day=%d/hour=%H'
            )
            returns: datetime.datetime(2015, 1, 9, 0, 0)

        """
        if isinstance(regex, basestring):
            regex = re.compile(regex)

        match = regex.search(path)
        if match:
            return datetime.datetime.strptime(match.group(1), format)
        else:
            logger.debug('No path matching {0} was found in {1}.'.format(regex.pattern, path))
            return None

    def query(self, query, check_return_code=True, use_tempfile=False):
        """
        Runs the given Hive query and returns stdout.

        Parameters:
            query             : The Hive query to run
            check_return_code : Passed to refinery.util.sh()
            use_tempfile      : If use_tempfile is True, the query will be written to
                                a temporary file and run as a Hive script.

        Returns:
            stdout output from Hive query
        """

        if use_tempfile:
            with tempfile.NamedTemporaryFile(prefix='tmp-hive-query-', suffix='.hiveql') as f:
                logger.debug('Writing Hive query to tempfile {0}.'.format(f.name))
                f.write(query)
                f.flush()
                out = self.script(f.name, check_return_code)
                # NamedTemporaryFile will be deleted on close().
            return out
        else:
            return self._command(['-e', query], check_return_code)

    def script(self, script, check_return_code=True):
        """Runs the contents of the given script in hive and returns stdout."""
        if not os.path.isfile(script):
            raise RuntimeError("Hive script: {0} does not exist.".format(script))
        return self._command(['-f', script], check_return_code)

    def _command(self, args, check_return_code=True):
        """Runs the `hive` from the command line, passing in the given args, and
           returning stdout.
        """
        cmd = self.hivecmd + args
        return sh(cmd, check_return_code)


class HivePartition(OrderedDict):
    partition_regex          = re.compile(r'(\w+)=["\']?([\w\-]+)["\']?')
    camus_regex              = re.compile(r'.*/hourly/(?P<year>\d+)\/(?P<month>\d+)\/(?P<day>\d+)\/(?P<hour>\d+)')

    desc_separator = '/'
    spec_separator = ','

    zfill_keys = {
        'year': 4,
        'month': 2,
        'day': 2,
        'hour': 2
    }

    def __init__(self, partition_string):

        # If we see an '=', assume this is a Hive style partition desc or spec.
        if '=' in partition_string:
            partitions = HivePartition.partition_regex.findall(partition_string)
        # Else assume this is a time bucketed camus imported path.
        # This only works with hourly camus data.
        else:
            match = HivePartition.camus_regex.search(partition_string)
            if match:
                partitions = []
                for key in ('year', 'month', 'day', 'hour'):
                    partitions.append((key, str(int(match.group(key)))))
            else:
                raise Exception(
                    'No path matching {0} was found in {1}.'.format(
                        HivePartition.camus_regex.pattern, partition_string
                    )
                )

        super(HivePartition, self).__init__(partitions)

    def datetime(self):
        """
        Returns a datetime.datetime for this partition.

        Supports different schemes, such as:
        year=...[/month=...[/day=...[/hour=...]]]
        date=YYYY-MM-DD
        month=YYYY-MM
        day=YYYY-MM-DD
        hour=YYYY-MM-DD-HH
        ...
        """
        # partitions can have non-datetime components
        relevant_partition_keys = set(['dt', 'date', 'year', 'month', 'day', 'hour', 'minute'])
        transformers = defaultdict(lambda: lambda x: x, {
            # 2018-5-15-05 is valid, but 2018-5-15-5 is not.  Prefix with 0 to mak
            # parser happy.
            'hour': lambda hour: "%02d" % (int(hour))
        })
        values = [transformers[k](self[k]) for k in self.keys() if k in relevant_partition_keys]

        # the date parser also only likes things that are zero-prefixed.
        # so
        return parser.parse(
            '-'.join(map(str, values)),
            fuzzy=True,
            default=datetime.datetime(2000, 1, 1, 0, 0)
        )

    def list(self, quote=False):
        """
        Returns a list of Hive partition key=value strings.
        IF quote=True, string values will be quoted.
        """
        partitions = []
        # Loop through each partition,
        # adding quotes around strings if quote=True
        for k, v in self.items():
            if quote and not v.isdigit():
                v = '\'{}\''.format(v)
            partitions.append('{}={}'.format(k, v))
        return partitions

    def desc(self):
        """
        Returns a Hive desc string, e.g. datacenter=eqiad/year=2017/month=11/day=21/hour=0
        """
        return HivePartition.desc_separator.join(self.list())

    def spec(self):
        """
        Returns a Hive spec string, e.g. datacenter='eqiad',year=2017,month=11,day=21,hour=0
        """
        return HivePartition.spec_separator.join(self.list(quote=True))

    def path(self, base_path=None):
        """
        Returns a path to the partition.  If base_path is given, it will be prefixed.
        """
        dirs = self.list()
        if base_path is not None:
            dirs = [base_path] + dirs
        return os.path.join(*dirs)

    def camus_path(self, base_path=None):
        """
        Returns a path to a keyless camus partition, e.g. 2017/02/05/00.
        If base_path is given, it will be prefixed.
        """
        dirs = [v.zfill(HivePartition.zfill_keys.get(k, 0)) for k, v in self.items()]
        if base_path is not None:
            dirs = [base_path] + dirs
        return os.path.join(*dirs)

    def glob(self, base_path=None):
        """
        Returns a file glob that would have matched this partition.
        This is just a handy way to build a file glob to match other partitions
        as deep as this one.  If base_path is given, it will be prefixed.
        """
        globs = ['*'] * len(self)
        if base_path is not None:
            globs = [base_path] + globs
        return os.path.join(*globs)


class HdfsUtils(object):
    @staticmethod
    def ls(paths, include_children=True, with_details=False):
        """
        Runs hdfs dfs -ls on paths.

        Parameters:
            paths            : List or string paths to files to ls.  Can include shell globs.
            include_children : If include_children is False, the -d flag will
                               be given to hdfs dfs -ls.
        Returns:
            Array of paths matching the ls-ed path.
        """

        if isinstance(paths, str):
            paths = paths.split()

        options = []
        if not include_children:
            options.append('-d')

        split_lines = [
            line.split() for line in sh(
                ['hdfs', 'dfs', '-ls'] + options + paths,
                # Not checking return code here so we don't
                # fail paths do not exist.
                check_return_code=False
            ).splitlines() if not line.startswith(b'Found ')
        ]

        if with_details:
            return [
                {
                    'file_type': 'f' if parts[0].decode('utf-8')[0] == '-' else 'd',
                    'permission': parts[0][1:],
                    'replication': parts[1],
                    'owner': parts[2],
                    'group': parts[3],
                    'file_size': parts[4],
                    'modification_date': parts[5],
                    'modification_time': parts[6],
                    'path': parts[7]
                } for parts in split_lines
            ]
        else:
            return [parts[-1] for parts in split_lines]

    @staticmethod
    def rm(paths, recurse=True, skip_trash=True):
        """
        Runs hdfs dfs -rm -R on paths, optinally skipping trash.
        """
        if isinstance(paths, str):
            paths = paths.split()

        options = (['-R'] if recurse else []) + (['-skipTrash'] if skip_trash else [])
        return sh(['hdfs', 'dfs', '-rm'] + options + paths)

    @staticmethod
    def rmdir(paths):
        """
        Runs hdfs dfs -rmdir on paths.
        """
        if isinstance(paths, str):
            paths = paths.split()

        return sh(['hdfs', 'dfs', '-rmdir'] + paths)

    def mkdir(paths, create_parent=True):
        """
        Runs hdfs dfs -mkdir -p on paths.
        """
        options = ['-p'] if create_parent else []
        if isinstance(paths, str):
            paths = paths.split()

        return sh(['hdfs', 'dfs', '-mkdir'] + options + paths)

    @staticmethod
    def cp(fromPath, toPath, force=False):
        """
        Runs 'hdfs dfs -cp fromPath toPath' to copy a file.
        """
        command = ['hdfs', 'dfs', '-cp', fromPath, toPath]
        if force:
            command.insert(3, '-f')
        sh(command)

    @staticmethod
    def mv(from_paths, to_paths, inParent=True):
        """
        Runs hdfs dfs -mv fromPath toPath for each values of from/to Paths.
        If inParent is True (default), the parent folder in each of the
        to_paths provide is used as destination. Set inParent parameter to
        False if the file/folder moved is also renamed.
        """
        if isinstance(from_paths, str):
            from_paths = from_paths.split()

        if isinstance(to_paths, str):
            to_paths = to_paths.split()

        if len(from_paths) != len(to_paths):
            raise Exception('from_paths and to_paths size don\'t match in hdfs mv function')

        for i in range(len(from_paths)) :
            toParent = '/'.join(to_paths[i].split('/')[:-1])
            if not HdfsUtils.ls(toParent, include_children=False):
                HdfsUtils.mkdir(toParent)
            if (inParent):
                sh(['hdfs', 'dfs', '-mv', from_paths[i], toParent])
            else:
                sh(['hdfs', 'dfs', '-mv', from_paths[i], to_paths[i]])

    @staticmethod
    def put(local_path, hdfs_path, force=False):
        """
        Runs 'hdfs dfs -put local_path hdfs_path' to copy a local file over to hdfs.
        """
        options = ['-f'] if force else []
        sh(['hdfs', 'dfs', '-put'] + options + [local_path, hdfs_path])

    @staticmethod
    def get(hdfs_path, local_path, force=False):
        """
        Runs 'hdfs dfs -get hdfs_path local_path' to copy a local file over to hdfs.
        """
        options = ['-f'] if force else []
        sh(['hdfs', 'dfs', '-get'] + options + [hdfs_path, local_path])

    @staticmethod
    def cat(path):
        """
        Runs hdfs dfs -cat path and returns the contents of the file.
        Be careful with file size, it will be returned as an in-memory string.
        """
        command = ['hdfs', 'dfs', '-cat', path]
        return sh(command).decode('utf-8')

    @staticmethod
    def get_modified_datetime(path):
        """
        Runs 'hdfs dfs -stat' and returns the modified datetime for the given path.
        """
        stat_str = sh(['hdfs', 'dfs', '-stat', path]).decode('utf-8')
        date_str, time_str = stat_str.strip().split()
        iso_datetime_str = date_str + 'T' + time_str + 'Z'
        return parser.parse(iso_datetime_str)

    @staticmethod
    def touchz(paths):
        """
        Runs hdfs dfs -touchz paths, optinally skipping trash.
        """
        if isinstance(paths, str):
            paths = paths.split()

        return sh(['hdfs', 'dfs', '-touchz'] + paths)

    @staticmethod
    def validate_path(path):
        return path.startswith('/') or path.startswith('hdfs://')

    @staticmethod
    def get_parent_dirs(path, top_parent_path):
        """
        Lists the parent directories of the specified path, going up the directory
        tree until top_parent_path is reached. This function expects top_parent_path
        to be a literal substring of path.
        """
        dirs = path.replace(top_parent_path, '')[1:].split('/')
        parents = [top_parent_path]
        for directory in dirs:
            parents.append(os.path.join(parents[-1], directory))
        parents.remove(path)
        return parents

    @staticmethod
    def dir_bytes_size(path):
        """
        Returns the size in bytes of a hdfs path
        """
        return int(sh(['hdfs', 'dfs', '-du', '-s', path]).split()[0])

    @staticmethod
    def rsync(local_path, hdfs_path, local_to_hdfs=True,
              should_delete=False, dry_run=True):
        """
        Copy files from source to destination (either local-to-hdfs or vice-versa
        depending on local_to_hdfs parameter) trying to minimise copies using
        file-size as a reference.
        Destination is checked to be an existing folder and source is expected
        to be a path or glob.
        Destination files present in source but whose size or type don't match
        source will be deleted and reimported.
        Both source and destination should be absolute.
        If should_delete is set to True, files in destination not present in source
        will be deleted.
        WARNING: If a glob pattern is used at folder-level, files will be copied
                 but not the folder hierarchy. This also means that if files have
                 the same name in different globed-folders, only one will remain
                 in the destination folder.

        """
        # Check destination being a folder
        if ((local_to_hdfs and not HdfsUtils._check_hdfs_dir(hdfs_path)) or
                (not local_to_hdfs and not HdfsUtils._check_local_dir(local_path))):
            return False

        # Get files to copy by comparing src and dst files lists
        files_left_to_copy = HdfsUtils._files_left_to_copy(
            local_path, hdfs_path, local_to_hdfs, should_delete, dry_run)
        if len(files_left_to_copy) == 0:
            logger.info('No file to copy'.format(len(files_left_to_copy)))
            return True

        # Define copy and dst_path depending on local_to_hdfs
        if local_to_hdfs:
            (copy, dst_path) = (HdfsUtils.put, hdfs_path)
        else:
            (copy, dst_path) = (HdfsUtils.get, local_path)

        # Do the actual copy
        logger.info('Copying {} files ...'.format(len(files_left_to_copy)))
        for f in files_left_to_copy:
            src_file = files_left_to_copy[f]['path']
            logger.info('Copying {} to {}'.format(src_file, dst_path))
            if not dry_run:
                copy(src_file, dst_path)

        # Check copy to return value
        files_left_to_copy = HdfsUtils._files_left_to_copy(
            local_path, hdfs_path, local_to_hdfs, should_delete, dry_run)
        if len(files_left_to_copy) == 0:
            logger.info('Successfull copy')
            return True
        else:
            logger.error('Copy not successfull, files are still to be copied: ' +
                         ','.join(files_left_to_copy))
            return False

    @staticmethod
    def _check_local_dir(local_path):
        """
        Returns True if given parameter is an existing local directory
        """
        if not os.path.isdir(local_path):
            logger.error('Local destination folder ' + local_path +
                         ' either doesn\'t exists or is not a directory')
            return False
        return True

    @staticmethod
    def _check_hdfs_dir(hdfs_path):
        """
        Returns True if given parameter is an existing hdfs directory
        """
        output_details = HdfsUtils.ls(hdfs_path, include_children=False, with_details=True)
        if not output_details or output_details[0]['file_type'] != 'd':
            logger.error('HDFS destination folder ' + hdfs_path +
                         ' either doesn\'t exists or is not a directory')
            return False
        return True

    @staticmethod
    def _files_left_to_copy(local_path, hdfs_path, local_to_hdfs,
                            should_delete, dry_run):
        """
        Builds the list of files to copy from source to destination.
        List files from source and destination and return only files
        that are in source and not in destination.
        A check on file-type and file-size is made for files being in source
        and destination. In case of a difference, destination file is overwritten.
        In case a file exists in destination folder but not in source, it is
        delete id should_delete is True, kept otherwise.
        """
        logger.info('Building lists of files to rsync from {} to {}'.format(
            local_path if local_to_hdfs else hdfs_path,
            hdfs_path if local_to_hdfs else local_path))

        # Get hdfs files
        hdfs_files = HdfsUtils._get_hdfs_files(hdfs_path)

        # Get local files using local_path as a glob except if it is a folder
        local_glob = os.path.join(local_path, '*') if os.path.isdir(local_path) else local_path
        local_files = HdfsUtils._get_local_files(local_glob)

        if local_to_hdfs:
            (src_files, dst_files) = (local_files, hdfs_files)
            rm = HdfsUtils.rm
        else:
            (src_files, dst_files) = (hdfs_files, local_files)
            rm = os.remove

        for existing_file in dst_files:
            dst_file = dst_files[existing_file]
            # file exists in src_files
            if existing_file in src_files:
                src_file = src_files[existing_file]
                # Same file-type and file-size (except for dirs)
                if (dst_file['file_type'] == src_file['file_type'] and
                        (dst_file['file_type'] == 'd' or
                            dst_file['file_size'] == src_file['file_size'])):
                    src_files.pop(existing_file, None)
                # Corrupted file - delete if should_delete or raise error
                else:
                    logger.info('Deleting {} '.format(dst_files[existing_file]['path']) +
                                'for being different from its source conterpart ' +
                                '(incorrect file type or size)')
                    if not dry_run:
                        rm(dst_files[existing_file]['path'])
            # file not present in to_be_imported list, delete if should_delete
            elif should_delete:
                logger.info('Deleting {} '.format(dst_files[existing_file]['path']) +
                            'for not being in the import list')
                if not dry_run:
                    rm(dst_files[existing_file]['path'])
            else:
                logger.info('File {} '.format(dst_files[existing_file]['path']) +
                            'is not in the import list')

        return src_files

    @staticmethod
    def _get_hdfs_files(hdfs_path):
        """
        List HDFS-files in path/glob parameter.
        Return a dictionnary keyed by filename and having dictionnary values
        of {'path', 'file_type', 'file_size'} (same as _get_local_files)
        """
        hdfs_files = {}
        # HDFS-ls works with path and globs
        for f in HdfsUtils.ls(hdfs_path, with_details=True):
            # HDFS-ls returns bytes - Need to extract
            path = f['path'].decode("utf-8")
            file_size = int(f['file_size'].decode("utf-8"))
            file_type = f['file_type']
            fname = os.path.basename(path)
            hdfs_files[fname] = {'path': path, 'file_size': file_size, 'file_type': file_type}
        return hdfs_files

    @staticmethod
    def _get_local_files(local_glob):
        """
        List local-files in path/glob parameter.
        Return a dictionnary keyed by filename and having dictionnary values
        of {'path', 'file_type', 'file_size'} (same as _get_hdfs_files)
        """
        local_files = {}
        for f in glob.glob(local_glob):
            fname = os.path.basename(f)
            file_size = os.path.getsize(f)
            file_type = 'd' if os.path.isdir(f) else 'f'
            local_files[fname] = {'path': f, 'file_size': file_size, 'file_type': file_type}
        return local_files


class DruidUtils(object):
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
