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
import logging
import os
import subprocess
import re
import tempfile
# Ugly but need python3 support
try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse

logger = logging.getLogger('refinery-util')


def is_yarn_application_running(job_name):
    '''
    Returns true if job_name is found in the output
    of yarn application -list and has a status of
    RUNNING or ACCEPTED.  Returns false otherwise.

    Note:  Error checking on the yarn shell command is not good.
    This command will return false on any command failure.
    '''
    command = '/usr/bin/yarn application -list 2>/dev/null | ' + \
        'grep -q  "\({0}\).*\(RUNNING\|ACCEPTED\)"'.format(job_name)
    logging.debug('Running: {0}'.format(command))
    retval = os.system(command)
    return retval == 0


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


    def table_exists(self, table): # ,force=False
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
        if not 'partitions' in self.tables[table].keys():
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
            (key,value) = p.split('=')
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
            int(match.groupdict().get('day',   1)),
            int(match.groupdict().get('hour',  0))
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
                path='/wmf/data/webrequest/webrequest_source=misc/year=2015/month=1/day=9/hour=0"
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
        return self._command( ['-f', script], check_return_code)


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
        l = []
        # Loop through each partition,
        # adding quotes around strings if quote=True
        for k, v in self.items():
            if quote and not v.isdigit():
                v = '\'{}\''.format(v)
            l.append('{}={}'.format(k, v))
        return l


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
    # TODO:  Use snakebite instead of shelling out to 'hdfs dfs'.

    @staticmethod
    def ls(paths, include_children=True):
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

        return [
            line.split()[-1] for line in sh(
                ['hdfs', 'dfs', '-ls'] + options + paths,
                # Not checking return code here so we don't
                # fail paths do not exist.
                check_return_code=False
            ).splitlines() if not line.startswith(b'Found ')
        ]

    @staticmethod
    def rm(paths):
        """
        Runs hdfs dfs -rm -R -skipTrash on paths.
        """
        if isinstance(paths, str):
            paths = paths.split()

        return sh(['hdfs', 'dfs', '-rm', '-R', '-skipTrash'] + paths)

    @staticmethod
    def mkdir(paths):
        """
        Runs hdfs dfs -rm -R on paths.
        """
        if isinstance(paths, str):
            paths = paths.split()

        return sh(['hdfs', 'dfs', '-mkdir', '-p'] + paths)

    @staticmethod
    def mv(fromPaths, toPaths, inParent=True):
        """
        Runs hdfs dfs -mv fromPath toPath for each values of from/to Paths.
        If inParent is True (default), the parent folder in each of the
        toPaths provide is used as destination. Set inParent parameter to
        False if the file/folder moved is also renamed.
        """
        if isinstance(fromPaths, str):
            fromPaths = fromPaths.split()

        if isinstance(toPaths, str):
            toPaths = toPaths.split()

        if len(fromPaths) != len(toPaths):
            raise Exception('fromPaths and toPaths size don\'t match in hdfs mv function')


        for i in range(len(fromPaths)) :
            toParent = '/'.join(toPaths[i].split('/')[:-1])
            if not HdfsUtils.ls(toParent, include_children=False):
                HdfsUtils.mkdir(toParent)
            if (inParent):
                sh(['hdfs', 'dfs', '-mv', fromPaths[i], toParent])
            else:
                sh(['hdfs', 'dfs', '-mv', fromPaths[i], toPaths[i]])

    @staticmethod
    def put(localPath, hdfsPath, force=False):
        """
        Runs 'hdfs dfs -put localPath hdfsPath' to copy a local file over to hdfs.
        """
        command = ['hdfs', 'dfs', '-put', localPath, hdfsPath]
        if force:
            command.insert(3, '-f')
        sh(command)

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
    def validate_path(path):
        return path.startswith('/') or path.startswith('hdfs://')
