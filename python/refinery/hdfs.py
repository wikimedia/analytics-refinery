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
Wikimedia Analytics Refinery python HDFS utility functions.

See util.py, hive.py, druid.py in the same folder
"""

from dateutil import parser
import logging
import os
import glob

from refinery.util import sh


logger = logging.getLogger('hdfs-util')


class Hdfs(object):
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
            ).splitlines() if not line.startswith('Found ')
        ]

        if with_details:
            return [
                {
                    'file_type': 'f' if parts[0][0] == '-' else 'd',
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
            if not Hdfs.ls(toParent, include_children=False):
                Hdfs.mkdir(toParent)
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
        return sh(command)

    @staticmethod
    def get_modified_datetime(path):
        """
        Runs 'hdfs dfs -stat' and returns the modified datetime for the given path.
        """
        stat_str = sh(['hdfs', 'dfs', '-stat', path])
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
        if ((local_to_hdfs and not Hdfs._check_hdfs_dir(hdfs_path)) or
                (not local_to_hdfs and not Hdfs._check_local_dir(local_path))):
            return False

        # Get files to copy by comparing src and dst files lists
        files_left_to_copy = Hdfs._files_left_to_copy(
            local_path, hdfs_path, local_to_hdfs, should_delete, dry_run)
        if len(files_left_to_copy) == 0:
            logger.info('No file to copy'.format(len(files_left_to_copy)))
            return True

        # Define copy and dst_path depending on local_to_hdfs
        if local_to_hdfs:
            (copy, dst_path) = (Hdfs.put, hdfs_path)
        else:
            (copy, dst_path) = (Hdfs.get, local_path)

        # Do the actual copy
        logger.info('Copying {} files ...'.format(len(files_left_to_copy)))
        for f in files_left_to_copy:
            src_file = files_left_to_copy[f]['path']
            logger.info('Copying {} to {}'.format(src_file, dst_path))
            if not dry_run:
                copy(src_file, dst_path)

        # Check copy to return value
        files_left_to_copy = Hdfs._files_left_to_copy(
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
        output_details = Hdfs.ls(hdfs_path, include_children=False, with_details=True)
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
        hdfs_files = Hdfs._get_hdfs_files(hdfs_path)

        # Get local files using local_path as a glob except if it is a folder
        local_glob = os.path.join(local_path, '*') if os.path.isdir(local_path) else local_path
        local_files = Hdfs._get_local_files(local_glob)

        if local_to_hdfs:
            (src_files, dst_files) = (local_files, hdfs_files)
            rm = Hdfs.rm
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
        for f in Hdfs.ls(hdfs_path, with_details=True):
            path = f['path']
            file_size = int(f['file_size'])
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
