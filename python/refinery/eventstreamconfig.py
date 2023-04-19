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
EventStreamConfig MediaWiki Extension API python client libary and CLI.

"""

import json
import requests
import re
import sys
import logging
from docopt import docopt

from refinery.util import flatten


logger = logging.getLogger('eventstreamconfig')

default_options = {
    'host': 'meta.wikimedia.org',
    'headers': {}
}

def streamconfigs_request(params={}, options={}):
    """
    Performs a GET request to EventStreamConfig API and returns the requests response.
    """
    options = {**default_options, **options}
    params = {
        'action': 'streamconfigs',
        'format': 'json',
        **params
    }

    url = 'https://{0}/w/api.php'.format(options['host'])
    return requests.get(url, params=params, headers=options['headers'])


def get_stream_configs(streams=None, constraints=None, options={}):
    """
    Gets the stream configs for streams, or all streams if not provided.
    """
    params = {}
    if streams:
        # Mediawiki API uses | as item separator in list GET param.
        params['streams'] = '|'.join(streams)
    if constraints:
        params['constraints'] = constraints

    r = streamconfigs_request(params, options)
    return r.json()['streams']



def get_topics_in_active_streams(streams=None, as_regex=False, constraints=None, options={}):
    """
    Helper function to calculate the topics that compose streams.
    This contains duplicated logic found in WMF eventgate configuration.

    All streams are composed of main WMF datacenter prefixed topics, except
    for legacy eventlogging_.* ones.

    Note that `wgEventStreams` in mediawiki-config can use regex stream
    name patterns.  This allows for easy configuration of streams that share
    the same configs, but also means that we don't have an explicit list of all streams
    anywhere.  For our use case, this is ok, as Kafka supports regex topic matching
    too.  The list of topics returned here might contain regexes.

    TODO: filter for what might be 'active' streams.  This is yet to be defined,
    but will likely be streams that have sampling rate greater than 0.
    """
    topic_prefixes = ['eqiad.', 'codfw.']
    no_prefix_pattern = re.compile(r'^eventlogging_.+')

    stream_configs = get_stream_configs(
        streams=streams,
        constraints=constraints,
        options=options
    )

    # Collect all topics settings from stream_configs.
    topics = flatten([settings['topics'] for settings in stream_configs.values()])
    if as_regex:
        topics_for_regex = []
        for topic in topics:
            # If topic is a regex, remove the surrounding '/', since we are
            # about to join it into a larger regex of all topics.
            topics_for_regex.append(topic.strip('/'))
        return '(' + '|'.join(topics_for_regex) + ')'
    else:
        return topics


def main():
    """
    EventStreamConfig MediaWiki Extension API python CLI.

    Usage: eventstreamconfig.py [options] [<streams>...]

    Options:
        -h --help                           Show this help message and exit.
        -u --host=<host>                    Mediwiki API hostname to request. [default: {0}]
        -C --constraints=<constraints>      A string like key1=val1|key2=val2 to pass to EventStreamConfig API
                                            as the constraints parameter.
                                            Example:
                                            'destination_event_service=eventgate-analytics'
        -H --headers=<headers>              JSON formatted object of HTTP headers to use when
                                            requesting stream config.
                                            Example:
                                            '{{"Host": "meta.wikimedia.org", "Other-Header": "value"}}'
    """
    arguments = docopt(main.__doc__.format(default_options['host']))

    headers = {}
    if arguments['--headers']:
        headers = json.loads(arguments['--headers'])

    stream_configs = get_stream_configs(
        arguments['<streams>'],
        arguments['--constraints'],
        {'headers': headers}
    )

    print(json.dumps(stream_configs))


if __name__ == '__main__':
    # Run tests.
    if len(sys.argv) > 1 and sys.argv[1] == 'test':

        import unittest
        import requests_mock

        stream_configs_url = 'https://{}/w/api.php?action=streamconfigs&format=json'.format(default_options['host'])
        mock_response_text = r'{"streams":{"eventlogging_SearchSatisfaction":{"stream":"eventlogging_SearchSatisfaction","schema_title":"analytics/legacy/searchsatisfaction","topics":["eventlogging_SearchSatisfaction"]},"test.event":{"stream":"test.event","schema_title":"test/event","topics":["eqiad.test.event","codfw.test.event"]},"/^mediawiki\\.job\\..+/":{"stream":"/^mediawiki\\.job\\..+/","schema_title":"error","topics":["/^(eqiad\\.|codfw\\.)mediawiki\\.job\\..+/"]}}}'

        @requests_mock.Mocker()
        class TestEventStreamConfig(unittest.TestCase):

            def test_get_stream_configs(self, rmock):
                rmock.get(stream_configs_url, text=mock_response_text)

                stream_configs = get_stream_configs(None)
                expected = json.loads(mock_response_text)['streams']

                self.assertEqual(stream_configs, expected)

            def test_get_topics_in_active_streams(self, rmock):
                rmock.get(stream_configs_url, text=mock_response_text)

                topics = get_topics_in_active_streams()
                expected = ['eventlogging_SearchSatisfaction', 'eqiad.test.event', 'codfw.test.event', '/^(eqiad\\.|codfw\\.)mediawiki\\.job\\..+/']

                self.assertEqual(topics, expected)

            def test_get_topics_in_active_streams_as_regex(self, rmock):
                rmock.get(stream_configs_url, text=mock_response_text)

                topics_regex = get_topics_in_active_streams(as_regex=True)
                expected = '(eventlogging_SearchSatisfaction|eqiad.test.event|codfw.test.event|^(eqiad\.|codfw\.)mediawiki\.job\..+)'

                self.assertEqual(topics_regex, expected)


        unittest.main(argv=[''])

    # Else run main CLI
    else:
        main()
