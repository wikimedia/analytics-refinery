#!/usr/bin/env python3

"""Uploads a directory to swift and optionally emits a swift/upload/complete event.
Usage:
    swift-upload [options] <swift_auth_file> <swift_container> <source_directory>

Options:
    -h --help                               Show this help message and exit.
    -p --swift-object-prefix=<prefix>       If not given, the basename of <source_directory> will be used.
    -d --swift-delete-after=<seconds>       The value of the Swift X-Delete-After header.
                                            [default: 7776000]
    -o --swift-overwrite=<true|false>       If given, any objects that have swift-object-prefix will be
                                            deleted before the upload begins. [default: false]
    -s --swift-storage-policy=<policy>      [default: lowlatency]
    -V --swift-auto-version=<true|false>    If given, a millisecond timestamp prefix will be prepended
                                            to the swift object prefix. This allows for every container
                                            upload to have a unique(ish) and sortable versioning.
                                            [default: false]
    -S --event-stream=<stream>              Set this to 'false' to disable sending of events. Otherwise,
                                            This will be used as the value of meta.stream in the produced
                                            swift/upload/complete event.  If not set, this
                                            will default to swift.<container>.upload-complete.
    -l --event-per-object=<true|false>      If set, each uploaded object will result in an event.
                                            If not set, there will be only one event per directory upload.
                                            In either case, the swift_upload_uri will be set to the URI
                                            you should query in order to get the list of objects
                                            for the event.  In the case where this is set, the
                                            ?prefix query parameter will match the full path of the
                                            object.  Otherwise it will match the path to the swift
                                            object prefix. [default: false]
    -u --upload-user=<upload_user>          Username of upload initiater.  Defaults to user running this process.
    -e --event-service-url=<url>            Event Service (e.g. EventGate) URI to POST event
                                            [default: https://eventgate-analytics.discovery.wmnet:4592/v1/events]
"""

__author__ = 'Andrew Otto <otto@wikimedia.org>'

from docopt import docopt
from datetime import datetime
import getpass
import io
import json
import logging
import os
import os.path as path
import shutil
import subprocess
import swiftclient
from swiftclient.service import SwiftService, SwiftUploadObject, SwiftError
import sys
import tempfile
from urllib import request
from urllib.error import HTTPError
from uuid import uuid4 as uuid


# Initialize a logging stream formatter for use in log messages.
logging_formatter = logging.Formatter('%(asctime)s %(levelname)-6s %(message)s', datefmt='%Y-%m-%dT%H:%M:%SZ')
logging_stream_handler = logging.StreamHandler()
logging_stream_handler.setFormatter(logging_formatter)


def dt_now():
    """
    Returns:
        date time microseconds in ISO-8601 with UTC timezone 'Z' suffix.
    """
    return datetime.utcnow().isoformat() + 'Z'


def get_lines_from_file(file):
    """
    Parameters:
        file    :   A str path or a file object. If file path starts with hdfs://, contents will
                    be captured from a subprocess, split, and then returned.
    Returns:
        lines in path
    """

    # If given a path to a file
    if isinstance(file, str):
        # If file is in hdfs, hdfs dfs -cat it out and use stdout
        if file.startswith('hdfs://'):
            command = ['hdfs', 'dfs', '-cat', file]
            process = subprocess.Popen(command, stdout=subprocess.PIPE)
            if process.wait() != 0:
                raise RuntimeError('Failed running command: {}.'.format(' '.join(comamnd)))
            file = process.stdout
        # else just open the file path
        else:
            file = open(file)

    return [l.strip() for l in file.readlines()]


def read_env_file(file):
    """
    Parameters:
        file    : A str path or a file object containing only bash export KEY=val statements.
    Returns:
        dict of key: val
    """
    lines = get_lines_from_file(file)
    # For all lines that start with export, dict of key: val
    return dict([
        tuple(l[7:].split('=')[0:2])
        for l in lines
        if l.startswith('export')
    ])


def source_env_file(file):
    """
    Sets os.environ for every export KEY=val line found in file.

    Parameters:
        file    : A str path or a file object containing only bash export KEY=val statements.
    Returns:
        dict of key: val
    """
    env_vars = read_env_file(file).items()
    for key, val in env_vars:
        os.environ[key] = val
    return env_vars


def hdfs_download_to_temp_dir(hdfs_directory, temp_prefix=None, temp_dir=None):
    """
    Downloads a directory from HDFS into a local temporary directory
    created with tempfile.mkdtemp.  This directory will not be automatically
    delete, you must do this yourself!

    Parameters:
        hdfs_directory  : HDFS directory path to download
        temp_prefix     : prefix given to tempfile.mkdtemp
        temp_dir        : temp_dir given to tempfile.mkdtemp
    Returns:
        Path to local temp dir in which hdfs_directory was downloaded
    """
    local_temp_dir = tempfile.mkdtemp(None, temp_prefix, temp_dir)
    subprocess.check_call(
        ['/usr/bin/hdfs', 'dfs', '-get', hdfs_directory, local_temp_dir]
    )
    return local_temp_dir


class SwiftHelper(object):
    """
    Wraps swiftclient.service.SwiftService to handle easy
    temp authentication with an env file; as well as common
    functions for listing and uploading directories.
    """
    def __init__(self, swiftservice):
        """
        Parameters:
            swiftservice    : Instantiated swiftclient.service.SwiftService
        """
        self.log = logging.getLogger('SwiftHelper')
        self.log.setLevel(logging.INFO)
        self.log.addHandler(logging_stream_handler)
        self.swiftservice = swiftservice

        # SwiftService doesn't have a great way to determine the storage url used.
        # We'll want to use this later to generate absolute URLs to objects.
        conn = swiftclient.service.get_conn(self.swiftservice._options)
        self.storage_url = conn.get_auth()[0]
        self.user = self.swiftservice._options['user']

    @staticmethod
    def factory(path):
        """
        Given a path to a swift tempauth env file,
        this will return an instantiated SwiftHelper

        Parameters:
            path    : path to a file containing swift temp auth bash export statements
        Returns:
            SwiftHelper
        """
        auth_opts = SwiftHelper.auth_file_to_opts(path)
        swiftservice = SwiftService(auth_opts)
        return SwiftHelper(swiftservice)

    @staticmethod
    def auth_file_to_opts(path):
        """
        Reads the swift auth file bash env file at path and returns
        opts dict suitable for passing to SwiftService constructor.

        Parameters:
            path    : path to a file containing swift temp auth bash export statements
        Returns:
            dict for passing to SwiftService constructor
        """
        env_vars = read_env_file(path)
        return {
            'auth': env_vars['ST_AUTH'],
            'user': env_vars['ST_USER'],
            'key': env_vars['ST_KEY']
        }

    def prefix_uri(self, container, object_prefix):
        """
        Retuns a URI which can be requested to get the list of objects that start
        with object_prefix.
        Parameters:
            container       : swift container
            object_prefix   : swift object prefix

        Returns:
            prefix URI
        """
        return '/'.join([self.storage_url, container]) + '?prefix={}'.format(object_prefix)

    def list(self, container, object_prefix=None, fully_qualified=False):
        """
        Parameters:
            container       : swift container
            object_prefix   : swift object prefix
            fully_qualified : If true, the objects will be prefixed with the swift storage_url
        Returns:
            list of object URIs
        """
        results = self.swiftservice.list(container, {'prefix': object_prefix})

        try:
            listings = next(
                r['listing'] for r in results
                if r['action'] == 'list_container_part' and 'listing' in r
            )
        except StopIteration:
            return []

        object_paths = [l['name'] for l in listings]
        if (fully_qualified):
            return ['/'.join([self.storage_url, container, p]) for p in object_paths]
        else:
            return object_paths

    def object_prefix_exists(self, container, object_prefix):
        """
        Parameters:
            container       : swift container
            object_prefix   : swift object prefix
        Returns
            True if any object exists in container with prefix, False otherwise.
        """
        return len(self.list(container, object_prefix)) > 0

    def delete_object_prefix(self, container, object_prefix):
        """
        Deletes all objects in container that have object prefix

        Parameters:
            container       : swift container
            object_prefix   : swift object prefix
        Returns
            Result of SwiftService.delete
        """
        self.log.info(
            'Deleting all objects in container {} with prefix {}'.format(
                container, object_prefix
            )
        )
        return list(self.swiftservice.delete(container, None, {'prefix': object_prefix}))

    def container_exists(
        self,
        container
    ):
        """
        Parameters:
            container       : swift container
        Returns:
            True if the container exists, False otherwise
        """
        try:
            self.swiftservice.stat(container)
            return True
        except SwiftError:
            return False

    def create_container(
        self,
        container,
        storage_policy='lowlatency',
        read_acl='.r:*,.rlistings'
    ):
        """
        Parameters:
            container       : swift container
            storage_policty : swift storage policy
            read_acl        : swift read ACL
        Returns:
            Result of swiftservice.post
        """
        options = {
            'header': ['X-Storage-Policy:{}'.format(storage_policy)],
            'read_acl': read_acl
        }
        self.log.info('Creating swift container {}'.format(container))

        response = dict(self.swiftservice.post(container, None, options))
        if not response['success']:
            raise RuntimeError('Failed creating swift container {}: {}'.format(
                container, response['error'])
            )

        return response

    def upload(
        self,
        source_directory,
        container,
        object_prefix,
        delete_after=7776000,
        overwrite=False
    ):
        """
        Parameters:
            source_directory : Path to directory to upload.  Supports local and hdfs directories.
            container        : swift container
            object_prefix    : swift object prefix
            delete_after     : value of X-Delete-After header
            overwrite        : If true, if any objects exist with object prefix,
                               they will first be deleted before the upload proceeds.
                               Otherwise, the upload will be aborted.
        Returns:
            Return value of CLI swift upload command
        """
        if self.object_prefix_exists(container, object_prefix):
            if not overwrite:
                self.log.error(
                    'Aborting swift upload from {} to container {} with object prefix {}; '.format(
                        source_directory, container, object_prefix
                    ) + 'object prefix already exists'
                )
                sys.exit(1)
            else:
                self.log.info('Swift container {} has objects with prefix {}; deleting before uploading'.format(
                    container, object_prefix
                ))
                self.delete_object_prefix(container, object_prefix)


        if not self.container_exists(container):
            self.create_container(container)

        self.log.info(
            'Uploading {} to swift container {} with object prefix {} ...'.format(
                source_directory, container, object_prefix
            )
        )

        try:
            # If not from hdfs, we will upload directly from source_directory.
            upload_from_directory = source_directory
            # If from hdfs, first download the source_directory
            # to a local temp dir and upload from there.
            if source_directory.startswith('hdfs://'):
                upload_from_directory = hdfs_download_to_temp_dir(
                    source_directory, 'swift-upload-{}-{}-'.format(
                        container, object_prefix
                    ).replace('/', '_')
                )
                self.log.info('Downloaded {} into local temp dir {}'.format(
                    source_directory, upload_from_directory
                ))

            # make sure upload_from_directory exists
            if not os.path.isdir(upload_from_directory):
                raise RuntimeError(
                    'Cannot upload to swift container {}, upload directory {} is not a directory'.format(
                        container,
                        upload_from_directory
                    )
                )

            # Unfortunetly, directory upload logic is in the swift upload shell CLI,
            # but not in the python SwiftService client.  Until
            # https://bugs.launchpad.net/python-swiftclient/+bug/1837794
            # is fixed, shell out to swift upload instead.
            swift_upload_command = [
                'swift',
                'upload',
                '--header',
                'X-Delete-After:{}'.format(delete_after),
                '--object-name',
                object_prefix,
                container,
                upload_from_directory
            ]

            # build TEMP AUTH env from swiftservice options for use in shell env
            env = {
                'ST_AUTH': self.swiftservice._options['auth'],
                'ST_USER': self.swiftservice._options['user'],
                'ST_KEY': self.swiftservice._options['key'],
                'PATH': os.environ.get('PATH', '/usr/local/bin:/usr/bin:/bin'),
            }

            self.log.info('Running swift upload command: {}'.format(' '.join(swift_upload_command)))
            return subprocess.check_call(swift_upload_command, env=env)

        finally:
            # delete upload_from_directory if it is a temp dir
            if source_directory != upload_from_directory:
                self.log.info('Deleting local temp dir {}'.format(upload_from_directory))
                shutil.rmtree(upload_from_directory)


def build_event(
    swift_user,
    swift_container,
    swift_object_prefix,
    swift_prefix_uri,
    stream=None,
    dt=None,
    upload_user=getpass.getuser(),
    schema='/swift/upload/complete/1.0.0',
):
    """
    Builds a swfit/upload/complete event.
    https://github.com/wikimedia/mediawiki-event-schemas/blob/master/jsonschema/swift/upload/complete/current.yaml

    Returns:
        Event dict
    """

    if not dt:
        dt = dt_now()

    # If not given a specific event stream name,
    # then default to swift.<container>.upload-complete
    if stream is None:
        stream = 'swift.{}.upload-complete'.format(swift_container)

    event = {
        '$schema': schema,
        'meta': {
            'stream': stream,
            'id': str(uuid()),
            'dt': dt,
        },
        'upload_user': upload_user,
        'swift_user': swift_user,
        'swift_container':  swift_container,
        'swift_object_prefix': swift_object_prefix,
        'swift_prefix_uri': swift_prefix_uri
    }
    return event


def build_events(
    swift,
    swift_container,
    swift_object_prefix,
    event_per_object=False,
    stream=None,
    dt=None,
    upload_user=getpass.getuser(),
    schema='/swift/upload/complete/1.0.0',
):
    """
    Builds a list of events.  If event_per_object is False, this list will be of size one,
    and its swift_prefix_uri will be set to match swift_object_prefix.
    If event_per_object is True, all of the objects in swift_object_prefix will be listed,
    and each one will result in an event. Each of those events will have swift_prefix_uri
    set to the full path to the object.  In either case, requesting swift_prefix_uri will
    return the list of objects for that event.
    """
    if event_per_object:
        swift_object_paths = swift.list(
            swift_container,
            swift_object_prefix,
        )
        swift_prefix_uris = [
            swift.prefix_uri(swift_container, path) for path in swift_object_paths
        ]
        return [
            build_event(
                swift.user,
                swift_container,
                swift_object_prefix,
                uri,
                stream,
                dt,
                upload_user,
                schema
            )
            for uri in swift_prefix_uris
        ]
    else:
        return [
            build_event(
                swift.user,
                swift_container,
                swift_object_prefix,
                swift.prefix_uri(swift_container, swift_object_prefix),
                stream,
                dt,
                upload_user,
                schema
            )
        ]


def http_post(uri, data, content_type=None):
    """
    POSTs data bytes to uri.

    Parameters:
        uri
        data            : bytes string
        content_type    : value of Content-Type header

    Returns:
        Response body
    """
    req = request.Request(uri, data)
    if content_type:
        req.add_header('Content-Type', content_type)

    try:
        response = request.urlopen(req)
    except HTTPError as err:
        raise RuntimeError(
            'Failed posting data to {}. Got {} {}\n{}'.format(
                uri, err.status, err.reason, err.read().decode('utf-8')
            ), err
        )

    if (response.status < 200 or response.status > 299):
        raise RuntimeError(
            'Failed posting data to {}. Got {} {}\n{}'.format(
                uri, response.status, response.reason, response.read().decode('utf-8')
            )
        )
    return response.read().decode('utf-8')


def emit_events(event_service_url, events):
    """
    Encodes event as json and POSTs it to event_service_url
    """
    data = json.dumps(events, sort_keys=True).encode('utf-8')
    return http_post(event_service_url, data, 'application/json')


if __name__ == '__main__':
    # parse arguments
    args = docopt(__doc__)

    log = logging.getLogger('swift-upload')
    log.setLevel(logging.INFO)
    log.addHandler(logging_stream_handler)

    log.info('Running swift-upload.py with args\n{}'.format(args))

    swift = SwiftHelper.factory(args['<swift_auth_file>'])

    # If no swift object prefix given, then just use basename opf source_directory
    swift_object_prefix = args['--swift-object-prefix'] or path.basename(args['<source_directory>'])

    now = dt_now()
    if (args['--swift-auto-version'] == 'true'):
        # Use a datetime for auto version prefixing.
        # This could be a hash or an actual semantic version, but
        # a millisecond UTC datetime is nicely sortable, and is
        # unlikely to collide for our use cases,
        # especially in any given container.
        swift_object_prefix = '/'.join([now, swift_object_prefix])

    swift.upload(
        args['<source_directory>'],
        args['<swift_container>'],
        swift_object_prefix,
        args['--swift-delete-after'],
        args['--swift-overwrite'] == 'true'
    )

    # If we are to emit event(s) for this upload
    if (args['--event-stream'] != 'false'):
        events = build_events(
            swift,
            args['<swift_container>'],
            swift_object_prefix,
            args['--event-per-object'] == 'true',
            # If this is None, it will be set using the container name
            args['--event-stream'],
            now
        )

        log.info('Emitting {} Swift upload complete events to stream {} via {}:\n{}'.format(
            len(events),
            events[0]['meta']['stream'],
            args['--event-service-url'],
            json.dumps(events, sort_keys=True)
        ))

        response = emit_events(args['--event-service-url'], events)
        print(response)



import unittest
class TestSwiftUpload(unittest.TestCase):
    def test_get_lines_from_file(self):
        expected = ['hello', 'there']
        buf = io.StringIO('hello\nthere\n')
        lines = get_lines_from_file(buf)
        self.assertEqual(lines, expected)

    def test_read_env_file(self):
        expected = {
            'ENV1': 'val1',
            'ENV2': 'val2',
        }
        buf = io.StringIO('export ENV1=val1\nexport ENV2=val2\n')
        lines = read_env_file(buf)
        self.assertEqual(lines, expected)

    def test_build_event(self):
        self.maxDiff = None
        expected = {
            '$schema': '/swift/upload/complete/1.0.0',
            'meta': {
                'dt': '2019-07-25T14:32:21.739219Z',
                'stream': 'swift.my-container.upload-complete',
            },
            'swift_user': 'analytics:admin',
            'upload_user': 'analytics',
            'swift_container': 'my-container',
            'swift_object_prefix': '2019-07-25T14:32:21.739219Z/prefix0',
            'swift_prefix_uri': 'https://ms-fe.svc.eqiad.wmnet/v1/AUTH_analytics/my-container?prefix=2019-07-25T14:32:21.739219Z/prefix0',
        }

        event = build_event(
            'analytics:admin',
            'my-container',
            '2019-07-25T14:32:21.739219Z/prefix0',
            'https://ms-fe.svc.eqiad.wmnet/v1/AUTH_analytics/my-container?prefix=2019-07-25T14:32:21.739219Z/prefix0',
            None,
            '2019-07-25T14:32:21.739219Z',
            'analytics',
        )
        # Id is uniquely generated each time, remove it for test comparison.
        del event['meta']['id']

        self.assertEqual(event, expected)


