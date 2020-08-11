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


def get_stream_configs(streams=None, all_settings=False, constraints=None, options={}):
    """
    Gets the stream configs for streams, or all streams if not provided.
    """
    params = {}
    if streams:
        # Mediawiki API uses | as item separator in list GET param.
        params['streams'] = '|'.join(streams)
    if all_settings:
        params['all_settings'] = 'true'
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
        all_settings=True,
        constraints=constraints,
        options=options
    )
    stream_names = stream_configs.keys()

    topics = []
    for stream_name in stream_names:
        caret_anchor = ''
        # if stream_name starts and ends with a '/' it is a regex stream pattern.
        # remove the '/' parts.
        if stream_name.startswith('/') and stream_name.endswith('/'):
            stream_name = stream_name[1:-1]
            # In case this is a regex that starts with ^, we need
            # to prefix after the ^.  To preserve the stream regex as intended,
            # we'll re-add the ^ at the beginning of the prefixed topic below.
            if stream_name.startswith('^'):
                caret_anchor = '^'
                stream_name = stream_name[1:]

        if re.match(no_prefix_pattern, stream_name):
            topics.append(caret_anchor + stream_name)
        else:
            for topic_prefix in topic_prefixes:
                topics.append(caret_anchor + topic_prefix + stream_name)

    if as_regex:
        return '(' + '|'.join(topics) + ')'
    else:
        return topics


def main():
    """
    EventStreamConfig MediaWiki Extension API python CLI.

    Usage: eventstreamconfig.py [options] [<streams>...]

    Options:
        -h --help                           Show this help message and exit.
        -u --host=<host>                    Mediwiki API hostname to request. [default: {0}]
        -A --all-settings                   Asks for all stream config settings to be returned.
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
        arguments['--all-settings'],
        arguments['--constraints'],
        {'headers': headers}
    )

    print(json.dumps(stream_configs))


if __name__ == '__main__':
    # Run tests.
    if len(sys.argv) > 1 and sys.argv[1] == 'test':

        import unittest
        import requests_mock

        stream_configs_url = 'https://{}/w/api.php?action=streamconfigs&format=json&all_settings=true'.format(default_options['host'])
        mock_response_text = r'{"streams":{"eventlogging_SearchSatisfaction":{"stream":"eventlogging_SearchSatisfaction","schema_title":"analytics/legacy/searchsatisfaction"},"test.event":{"stream":"test.event","schema_title":"test/event"},"/^eventgate-.+\\.error(\\..+)?/":{"stream":"/^eventgate-.+\\.error(\\..+)?/","schema_title":"error"}}}'

        @requests_mock.Mocker()
        class TestEventStreamConfig(unittest.TestCase):

            def test_get_stream_configs(self, rmock):
                rmock.get(stream_configs_url, text=mock_response_text)

                stream_configs = get_stream_configs(None, True)
                expected = json.loads(mock_response_text)['streams']

                self.assertEqual(stream_configs, expected)

            def test_get_topics_in_active_streams(self, rmock):
                rmock.get(stream_configs_url, text=mock_response_text)

                topics = get_topics_in_active_streams()
                expected = ['eventlogging_SearchSatisfaction', 'eqiad.test.event', 'codfw.test.event', '^eqiad.eventgate-.+\\.error(\\..+)?', '^codfw.eventgate-.+\\.error(\\..+)?']

                self.assertEqual(topics, expected)

            def test_get_topics_in_active_streams_as_regex(self, rmock):
                rmock.get(stream_configs_url, text=mock_response_text)

                topics_regex = get_topics_in_active_streams(as_regex=True)
                expected = '(eventlogging_SearchSatisfaction|eqiad.test.event|codfw.test.event|^eqiad.eventgate-.+\.error(\..+)?|^codfw.eventgate-.+\.error(\..+)?)'

                self.assertEqual(topics_regex, expected)


        unittest.main(argv=[''])

    # Else run main CLI
    else:
        main()
