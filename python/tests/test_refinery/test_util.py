from unittest import TestCase
from datetime import datetime, timedelta
from refinery.util import HiveUtils, HdfsUtils, sh
import os


class TestReinferyUtil(TestCase):
    def test_sh(self):
        command = ['/bin/echo', 'test-list']
        output = sh(command)
        self.assertEqual(output, 'test-list')

        command = '/bin/echo test-string'
        output = sh(command)
        self.assertEqual(output, 'test-string')

    def test_sh_pipe(self):
        command = '/bin/echo hi_there | /usr/bin/env sed -e \'s@_there@_you@\''
        output = sh(command)
        self.assertEqual(output, 'hi_you')

class TestHiveUtil(TestCase):
    def setUp(self):
        self.hive = HiveUtils()
        self.hive.tables = {
            'table1': {
                'metadata': {
                    'Location':      'hdfs://test.example.com:8020/path/to/table1',
                }
            },
        }

        self.table_info = {
            'table1': {
                'location':             '/path/to/table1',
                'partitions_desc':      ['webrequest_source=mobile/year=2013/month=10/day=01/hour=01', 'webrequest_source=mobile/year=2013/month=10/day=01/hour=02'],
                'partitions_spec':      ['webrequest_source=\'mobile\',year=2013,month=10,day=01,hour=01', 'webrequest_source=\'mobile\',year=2013,month=10,day=01,hour=02'],
                'partitions_datetime':  [datetime(2013,10,01,01), datetime(2013,10,01,02)],
                'partitions_path':      ['/path/to/table1/webrequest_mobile/hourly/2013/10/01/01', '/path/to/table1/webrequest_mobile/hourly/2013/10/01/02'],
            },
        }

    def test_reset(self):
        self.hive.reset()
        self.assertEqual(self.hive.tables, {})

    def test_table_exists(self):
        self.assertTrue(self.hive.table_exists('table1'))
        self.assertFalse(self.hive.table_exists('nonya'))

    def test_table_location(self):
        self.assertEquals(self.hive.table_location('table1'), self.hive.tables['table1']['metadata']['Location'])
        self.assertEquals(self.hive.table_location('table1', strip_nameservice=True), '/path/to/table1')

    def test_partition_spec_from_partition_desc(self):
        expect = self.table_info['table1']['partitions_spec'][0]

        spec   = HiveUtils.partition_spec_from_partition_desc(self.table_info['table1']['partitions_desc'][0])
        self.assertEqual(spec, expect)

    def test_partition_spec_from_path(self):
        expect = self.table_info['table1']['partitions_spec'][0]
        path   = self.table_info['table1']['partitions_path'][0]
        regex  = r'/webrequest_(?P<webrequest_source>[^/]+)/hourly/(?P<year>[^/]+)/(?P<month>[^/]+)/(?P<day>[^/]+)/(?P<hour>[^/]+)'

        spec = HiveUtils.partition_spec_from_path(path, regex)
        self.assertEqual(spec, expect)

    def test_partition_datetime_from_spec(self):
        expect = self.table_info['table1']['partitions_datetime'][0]
        spec   = self.table_info['table1']['partitions_spec'][0]
        regex  = r'webrequest_source=(?P<webrequest_source>[^/,]+)[/,]year=(?P<year>[^/,]+)[/,]month=(?P<month>[^/,]+)[/,]day=(?P<day>[^/]+)[/,]hour=(?P<hour>[^/,]+)'

        dt     = HiveUtils.partition_datetime_from_spec(spec, regex)
        self.assertEqual(dt, expect)

    def test_partition_datetime_from_path(self):
        expect = self.table_info['table1']['partitions_datetime'][0]
        path   = self.table_info['table1']['partitions_path'][0]
        regex  = r'.*/hourly/(.+)$'

        dt     = HiveUtils.partition_datetime_from_path(path, regex)
        self.assertEqual(dt, expect)

    def test_drop_partitions_ddl(self):
        partition_ddls = ['PARTITION ({0})'.format(spec)
            for spec in self.table_info['table1']['partitions_spec']
        ]
        expect = '\n'.join(['ALTER TABLE {0} DROP IF EXISTS {1};'.format('table1', partition_ddl) for partition_ddl in partition_ddls])

        statement = self.hive.drop_partitions_ddl('table1', self.table_info['table1']['partitions_spec'])
        self.assertEqual(statement, expect)

