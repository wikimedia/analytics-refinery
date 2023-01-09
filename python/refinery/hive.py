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
Wikimedia Analytics Refinery python Hive utility functions.

See util.py, hdfs.py, druid.py in the same folder
"""

from collections import defaultdict, OrderedDict
from dateutil import parser
import datetime
import logging
import os
import re
import tempfile

from urllib.parse import urlparse
from refinery.util import sh


logger = logging.getLogger('hive-util')


class Hive(object):
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
        for p in desc.split(Hive.partition_desc_separator):
            (key, value) = p.split('=')
            if not value.isdigit():
                value = '\'{0}\''.format(value)
            spec_parts.append('{0}={1}'.format(key, value))

        # Replace partition_desc_separators with partition_spec_separators.
        return Hive.partition_spec_separator.join(spec_parts)

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
        if isinstance(regex, str):
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

        return Hive.partition_spec_separator.join(spec_parts)

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
        if isinstance(regex, str):
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
        if isinstance(regex, str):
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
            with tempfile.NamedTemporaryFile(mode='w', prefix='tmp-hive-query-', suffix='.hiveql') as f:
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
    partition_regex          = re.compile(r'(\w+)=["\']?([\w\-.]+)["\']?')
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

        It also supports 'snapshot' partitions, a concept which in our datalake has been overloaded
        to mean the equivalent of a 'week' in some tables, and a 'month' in others. Examples:
        snapshot=YYYY-MM-DD    (represents the start of the week)
        snapshot=YYYY-MM       (represents the start of the month)
        ...
        """
        # partitions can have non-datetime components
        relevant_partition_keys = set(['snapshot', 'dt', 'date', 'year', 'month', 'day', 'hour', 'minute'])
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

    def list(self, hql=False):
        """
        Returns a list of Hive partition key=value strings.
        IF hql=True keys are quoted with back-ticks and values
        are single-quoted unless composed of only digits making
        this a valid hql expression.
        """
        partitions = []
        for k, v in self.items():
            if hql:
                k = '`{}`'.format(k)
                if not v.isdigit():
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
        Returns a Hive spec string, e.g. `datacenter`='eqiad',`year`=2017,`month`=11,`day`=21,`hour`=0
        """
        return HivePartition.spec_separator.join(self.list(hql=True))

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

    def contains_snapshot(self):
        """
        Checks whether this partition includes a snapshot component. Examples:
        'snapshot=2022-12-05/wiki=enwiki' would return True, while
        'year=2022/wiki=enwiki' would return False.
        """
        return "snapshot" in self.keys()

    def snapshot_period(self):
        """
        Snapshot partitions are overloaded and can represent a week or a month.
        This function returns the period that snapshot represents. Examples:
        'snapshot=2022-12-05/wiki=enwiki' returns 'week'
        'snapshot=2022-12/wiki=enwiki' returns 'month'
        'year=2022/wiki=enwiki' returns None
        """
        if self.contains_snapshot():
            if re.search("^\d{4}-\d{2}-\d{2}$", self.get("snapshot")):  # Ex 2022-01-01
                return 'week'
            elif re.search("^\d{4}-\d{2}$", self.get("snapshot")):  # Ex 2022-01
                return 'month'
            else:
                return None
        else:
            return None
