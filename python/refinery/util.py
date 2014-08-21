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

import datetime
from dateutil.parser import parse as dateutil_parse
import logging
import os
import subprocess
import re
import tempfile
from urlparse import urlparse

logger = logging.getLogger('refinery-util')


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


    def partitions(self, table):
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
                path='/wmf/data/raw/webrequest/webrequest_mobile/hourly/2014/05/14/23',
                regex=r'/webrequest_(?P<webrequest_source>[^/]+)/hourly/(?P<year>[^/]+)/(?P<month>[^/]+)/(?P<day>[^/]+)/(?P<hour>[^/]+)'
            )
            returns: 'webrequest_source='mobile',year=2014,month=05,day=14,hour=23
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
                spec='webrequest_source=\'mobile\',year=2014,month=05,day=14,hour=00',
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
    def partition_datetime_from_path(path, regex):
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
        Returns:
            datetime object matching this spec's date.

        Example:
            partition_datetime_from_path(
                path='/wmf/data/raw/webrequest/webrequest_mobile/hourly/2014/05/14/23',
                regex=r'.*/hourly/(.+)$'
            )
            returns: datetime.datetime(2014, 5, 14, 23, 0)

        """
        if isinstance(regex, basestring):
            regex = re.compile(regex)

        return dateutil_parse(regex.search(path).group(1))


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
            ).splitlines() if not line.startswith('Found ')
        ]


    @staticmethod
    def rm(paths):
        """
        Runs hdfs dfs -rm -R on paths.
        """
        if isinstance(paths, str):
            paths = paths.split()

        return sh(['hdfs', 'dfs', '-rm', '-R'] + paths)

    @staticmethod
    def validate_path(path):
        return path.startswith('/') or path.startswith('hdfs://')

