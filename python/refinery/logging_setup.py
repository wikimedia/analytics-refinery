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


'''
Wikimedia Analytics Refinery python logging utility functions.
'''

import sys
import logging


def configure_logging(logger, level, log_file=None, stdout=False):
    # This should not be called twice, log an error and return
    if len(logger.handlers):
        logger.error('LOGGING ALREADY SET UP BUT configure_logging CALLED AGAIN')
        return

    logger.setLevel(level)

    formatter = logging.Formatter(
        fmt='%(asctime)s %(levelname)-6s %(message)s',
        datefmt='%Y-%m-%dT%H:%M:%S',
    )

    if log_file:
        # Log log_level and above to a file, if specified
        log_file_handler = logging.FileHandler(log_file)
        log_file_handler.setFormatter(formatter)
        logger.addHandler(log_file_handler)

    if stdout:
        # Log log_level and above to stdout, if specified
        log_stdout_handler = logging.StreamHandler(sys.stdout)
        log_stdout_handler.setFormatter(formatter)
        logger.addHandler(log_stdout_handler)

    # In addition, log warning and above to stderr
    error_handler = logging.StreamHandler(sys.stderr)
    error_handler.setLevel(logging.WARN)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)
