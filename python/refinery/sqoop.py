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
Wikimedia Analytics Refinery sqoop python helpers
"""
import sys
import logging
import re

from subprocess import check_call, DEVNULL
from refinery.hdfs import Hdfs
from refinery.util import is_yarn_application_running, get_dbnames_from_mw_config

logger = logging.getLogger()


class SqoopConfig:

    def __init__(self, yarn_job_name_prefix,
                 user, password_file, jdbc_string,
                 num_mappers, fetch_size, output_format, tmp_base_path,
                 table_path_template, dbname, table, queries,
                 target_jar_dir, jar_file, yarn_queue, driver_class,
                 current_try, dry_run):

        self.yarn_job_name_prefix = yarn_job_name_prefix
        self.user = user
        self.password_file = password_file
        self.jdbc_string = jdbc_string
        self.num_mappers_base = num_mappers
        self.num_mappers_weighted = max(int(num_mappers * queries[table]['mappers-weight']), 1)
        self.fetch_size = fetch_size
        self.output_format = output_format
        self.tmp_base_path = tmp_base_path
        self.table_path_template = table_path_template
        self.dbname = dbname
        self.table = table
        self.query = queries[table].get('query')
        self.boundary_query = queries[table]['boundary-query'] if ('boundary-query' in queries[table]) else None
        self.split_by = queries[table]['split-by'] if ('split-by' in queries[table]) else None
        self.map_types = queries[table]['map-types'] if ('map-types' in queries[table]) else None
        # If sqoopable_dbnames is not defined for this table, it means there is no restriction
        # on dbnames for that table, meaning all dbnames are sqoopable.
        self.is_sqoopable = (('sqoopable_dbnames' not in queries[table]) or (dbname in queries[table]['sqoopable_dbnames']))
        self.target_jar_dir = target_jar_dir
        self.jar_file = jar_file
        self.yarn_queue = yarn_queue
        self.driver_class = driver_class
        self.current_try = current_try
        self.dry_run = dry_run

    def __str__(self):
        return self.dbname + ':' + self.table


def sqoop_wiki(config):
    """
    Run a single sqoop import (1 database, 1 table)

    Parameters
        config: SqoopConfig object filed in with needed parameters

    Returns
        None if the sqoop worked
        The config object with updated try number if the sqoop errored or failed in any way
    """
    if not config.is_sqoopable:
        logger.info('SKIPPING: Table {} is not to be sqooped for database {}'.format(config.table, config.dbname))
        return None

    full_table = '.'.join([config.dbname, config.table])
    log_message = '{} (try {})'.format(full_table, config.current_try)
    logger.info('STARTING: {}'.format(log_message))
    try:
        query = config.query
        command = 'import'
        if config.target_jar_dir:
            query = query + ' and 1=0'
            command = 'codegen'

        sqoop_arguments = [
            'sqoop',
            command,
            '-D'                , "mapred.job.name='{}-{}'".format(
                config.yarn_job_name_prefix, full_table),
            '-D'                , "mapreduce.job.queuename={}".format(config.yarn_queue),
            '--username'        , config.user,
            '--password-file'   , config.password_file,
            '--connect'         , config.jdbc_string,
            '--query'           , query,
        ]

        if config.driver_class:
            sqoop_arguments += ['--driver' , config.driver_class]

        if config.target_jar_dir:
            sqoop_arguments += [
                '--class-name'      , config.table,
                '--outdir'          , config.target_jar_dir,
                '--bindir'          , config.target_jar_dir,
            ]
        else:
            # We don't use the hive-partition folder style since
            # it fails when sqooping as parquet outpout format.
            # We instead sqoop into a temporary folder, and then
            # move it to the correct place

            target_directory = (config.table_path_template + '/wiki_db={db}').format(
                table=config.table, db=config.dbname)

            tmp_target_directory = table_path_to_tmp_path(target_directory, config.tmp_base_path)

            sqoop_arguments += [
                '--target-dir'      , tmp_target_directory,
                '--num-mappers'     , str(config.num_mappers_weighted),
                '--as-{}file'.format(config.output_format),
            ]

            if config.fetch_size:
                sqoop_arguments += ['--fetch-size', str(config.fetch_size)]

            if config.num_mappers_weighted > 1:
                if config.boundary_query:
                    sqoop_arguments += ['--boundary-query', config.boundary_query]
                # if num_mappers_weighted <= 1, split_by can be None, otherwise it should be set
                sqoop_arguments += ['--split-by', config.split_by]

        if config.jar_file:
            sqoop_arguments += [
                '--class-name'      , config.table,
                '--jar-file'        , config.jar_file,
            ]

        if config.map_types:
            sqoop_arguments += [
                '--map-column-java' , config.map_types
            ]

        # Force deletion of possibly existing dir in case of retry
        if config.current_try > 1:
            sqoop_arguments += [
                '--delete-target-dir'
            ]

        # This step is needed particularly in case of retry - tmp_target_directory
        # Folder is created at first try, so we need to delete it (if it exists)
        # before trying again.
        if not config.target_jar_dir:
            logger.info('Deleting temporary target directory {} if it exists'.format(tmp_target_directory))
            if not config.dry_run:
                try:
                    Hdfs.rm(tmp_target_directory)
                except(Exception):
                    pass

        logger.debug('Sqooping with: {}'.format(sqoop_arguments))
        logger.debug('You can copy the parameters above and execute the sqoop command manually')
        # Ignore sqoop output because it's in Yarn and grabbing output is way complicated
        if not config.dry_run:
            check_call(sqoop_arguments, stdout=DEVNULL, stderr=DEVNULL)
        if not config.target_jar_dir:
            logger.info('Moving sqooped folder from {} to {}'.format(tmp_target_directory, target_directory))
            if not config.dry_run:
                Hdfs.mv(tmp_target_directory, target_directory, inParent=False)
        logger.info('FINISHED: {}'.format(log_message))
        return None
    except(Exception):
        logger.exception('ERROR: {}'.format(log_message))
        config.current_try += 1
        return config


def make_timestamp_clause(field_name, from_timestamp, to_timestamp):
    """
    Returns a valid SQL boolean clause using the timetamp_field as field-name
    and from_timestamp and to_timestamp as limit values if defined
    """
    timestamp_clause = ''
    if from_timestamp:
        timestamp_clause += '''  and {n} >= '{f}' '''.format(n=field_name, f=from_timestamp)
    if to_timestamp:
        timestamp_clause += '''  and {n} < '{t}' '''.format(n=field_name, t=to_timestamp)
    return timestamp_clause


def validate_tables_and_get_queries(filter_tables, from_timestamp, to_timestamp):
    """
    Returns a dictionary of mediawiki table names to the correct sqoop
    query to execute for import.

    Notes
        - convert(... using utf8mb4) is used to decode varbinary fields into strings
        - map-types is used to handle some databases having booleans in
          tinyint(1) and others in tinyint(3,4) (newer databases like wikivoyage)
        - from_timestamp and to_timestamp are optional

    Parameters
        filter_tables: list of tables to return queries for, None for all tables
        from_timestamp: imported timestamps must be newer than this, (YYYYMMDDHHmmss)
        to_timestamp: timestamps must be *strictly* older than this, (YYYYMMDDHHmmss)

    Returns
        An object of the form:
            {
                'table-name': {
                    'query'         : <<the sql query to run on mysql>>,
                    'boundary-query': <<the sql query to get min and max values for split-by>>,
                    'split-by'      : <<the column to use to parallelize imports>>,
                    'map-types'     : <<any info to pass as --map-column-java>>,
                },
                ...
            }
    """
    queries = {}

    ############################################################################
    # Tables sqooped from labs (usually)
    ############################################################################

    queries['archive'] = {
        'query': '''
             select ar_id,
                    ar_namespace,
                    convert(ar_title using utf8mb4) ar_title,
                    null ar_text,
                    null ar_comment,
                    null ar_user,
                    null ar_user_text,
                    convert(ar_timestamp using utf8mb4) ar_timestamp,
                    ar_minor_edit,
                    null ar_flags,
                    ar_rev_id,
                    null ar_text_id,
                    ar_deleted,
                    ar_len,
                    ar_page_id,
                    ar_parent_id,
                    convert(ar_sha1 using utf8mb4) ar_sha1,
                    null ar_content_model,
                    null ar_content_format,
                    ar_actor,
                    ar_comment_id

               from archive
              where $CONDITIONS
                {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('ar_timestamp', from_timestamp, to_timestamp)),
        'map-types': '"{}"'.format(','.join([
            'ar_actor=Long',
            'ar_comment=String',
            'ar_comment_id=Long',
            'ar_content_format=String',
            'ar_content_model=String',
            'ar_deleted=Integer',
            'ar_flags=String',
            'ar_minor_edit=Boolean',
            'ar_text=String',
            'ar_user=Long',
            'ar_user_text=String',
            'ar_text_id=Long',
        ])),
        'boundary-query': '''
            SELECT MIN(ar_id),
                   MAX(ar_id)
              FROM archive
             WHERE TRUE
                 {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('ar_timestamp', from_timestamp, to_timestamp)),
        'split-by': 'ar_id',
        'mappers-weight': 0.5,
    }


    queries['category'] = {
        'query': '''
             select cat_id,
                    convert(cat_title using utf8mb4) cat_title,
                    cat_pages,
                    cat_subcats,
                    cat_files

               from category
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'cat_id=Long',
            'cat_title=String',
            'cat_pages=Integer',
            'cat_subcats=Integer',
            'cat_files=Integer',
        ])),
        'boundary-query': 'SELECT MIN(cat_id), MAX(cat_id) FROM category',
        'split-by': 'cat_id',
        'mappers-weight': 0.25,
    }


    queries['categorylinks'] = {
        'query': '''
             select cl_from,
                    convert(cl_to using utf8mb4) cl_to,
                    convert(cl_sortkey using utf8mb4) cl_sortkey,
                    convert(cl_sortkey_prefix using utf8mb4) cl_sortkey_prefix,
                    convert(cl_timestamp using utf8mb4) cl_timestamp,
                    convert(cl_collation using utf8mb4) cl_collation,
                    convert(cl_type using utf8mb4) cl_type

               from categorylinks
              where $CONDITIONS
                {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('cl_timestamp', from_timestamp, to_timestamp)),
        'map-types': '"{}"'.format(','.join([
            'cl_from=Long',
            'cl_to=String',
            'cl_sortkey=String',
            'cl_sortkey_prefix=String',
            'cl_timestamp=String',
            'cl_collation=String',
            'cl_type=String',
        ])),
        'boundary-query': '''
            SELECT MIN(cl_from),
                   MAX(cl_from)
              FROM categorylinks
             WHERE TRUE
                 {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('cl_timestamp', from_timestamp, to_timestamp)),
        'split-by': 'cl_from',
        'mappers-weight': 1.0,
    }

    queries['change_tag'] = {
        'query': '''
             select ct_id,
                    ct_log_id,
                    ct_rev_id,
                    ct_tag_id,
                    convert(ct_params using utf8mb4) ct_params

               from change_tag
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'ct_id=Long',
            'ct_log_id=Long',
            'ct_rev_id=Long',
            'ct_tag_id=Long',
        ])),
        'boundary-query': 'SELECT MIN(ct_id), MAX(ct_id) FROM change_tag',
        'split-by': 'ct_id',
        'mappers-weight': 0.5,
    }

    queries['change_tag_def'] = {
        'query': '''
             select ctd_id,
                    convert(ctd_name using utf8mb4) ctd_name,
                    ctd_user_defined,
                    ctd_count

               from change_tag_def
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'ctd_count=Long',
            'ctd_id=Long',
            'ctd_user_defined=Boolean',
        ])),
        'boundary-query': 'SELECT MIN(ctd_id), MAX(ctd_id) FROM change_tag_def',
        'split-by': 'ctd_id',
        'mappers-weight': 0.0,
    }

    queries['content'] = {
        'query': '''
             select content_id,
                    content_size,
                    convert(content_sha1 using utf8mb4) content_sha1,
                    content_model,
                    convert(content_address using utf8mb4) content_address

               from content
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'content_id=Long',
            'content_size=Integer',
            'content_model=Integer',
        ])),
        'boundary-query': 'SELECT MIN(content_id), MAX(content_id) FROM content',
        'split-by': 'content_id',
        'mappers-weight': 1.0,
        # Sqooping content table for commonswiki and etwiki only for now
        # https://phabricator.wikimedia.org/T238878
        # Note: etwiki is needed as we build ORM jar from it
        'sqoopable_dbnames': ['commonswiki', 'etwiki']
    }

    queries['content_models'] = {
        'query': '''
             select model_id,
                    convert(model_name using utf8mb4) model_name

               from content_models
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'model_id=Integer',
        ])),
        'boundary-query': 'SELECT MIN(model_id), MAX(model_id) FROM content_models',
        'split-by': 'model_id',
        'mappers-weight': 0.0,
    }

    queries['externallinks'] = {
        'query': '''
             select el_id,
                    el_from,
                    convert(el_to_domain_index using utf8mb4) el_to_domain_index,
                    convert(el_to_path using utf8mb4) el_to_path

               from externallinks
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'el_id=Long',
            'el_from=Long',
            'el_to_domain_index=String',
            'el_to_path=String'
        ])),
        'boundary-query': 'SELECT MIN(el_id), MAX(el_id) FROM externallinks',
        'split-by': 'el_id',
        'mappers-weight': 1.0,
    }

    queries['image'] = {
        'query': '''
             select convert(img_name using utf8mb4) img_name,
                    img_size,
                    img_width,
                    img_height,
                    -- Field not sqooped as it can contain more than 10Mb of data
                    -- leading to job failure (commonswiki database only)
                    -- convert(img_metadata using utf8mb4) img_metadata,
                    img_bits,
                    convert(img_media_type using utf8mb4) img_media_type,
                    convert(img_major_mime using utf8mb4) img_major_mime,
                    convert(img_minor_mime using utf8mb4) img_minor_mime,
                    img_description_id,
                    img_actor,
                    convert(img_timestamp using utf8mb4) img_timestamp,
                    convert(img_sha1 using utf8mb4) img_sha1

               from image
              where $CONDITIONS
                {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('img_timestamp', from_timestamp, to_timestamp)),
        'map-types': '"{}"'.format(','.join([
            'img_name=String',
            'img_size=Long',
            'img_width=Integer',
            'img_height=Integer',
            #'img_metadata=String',
            'img_bits=Integer',
            'img_media_type=String',
            'img_major_mime=String',
            'img_minor_mime=String',
            'img_description_id=Long',
            'img_actor=Long',
            'img_timestamp=String',
            'img_sha1=String',
        ])),
        # Forcing single mapper to prevent having to split-by as table's primary-key
        # is a varchar (complicated to split). Data-size is not big even for commonswiki
        # so single-mapper does the job.
        'mappers-weight': 0.0,
    }

    queries['imagelinks'] = {
        'query': '''
             select il_from,
                    convert(il_to using utf8mb4) il_to,
                    il_from_namespace

               from imagelinks
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(il_from), MAX(il_from) FROM imagelinks',
        'split-by': 'il_from',
        'mappers-weight': 0.25,
    }

    queries['ipblocks'] = {
        'query': '''
             select ipb_id,
                    convert(ipb_address using utf8mb4) ipb_address,
                    ipb_user,
                    null ipb_by,
                    null ipb_by_text,
                    null ipb_reason,
                    convert(ipb_timestamp using utf8mb4) ipb_timestamp,
                    ipb_auto,
                    ipb_anon_only,
                    ipb_create_account,
                    ipb_enable_autoblock,
                    convert(ipb_expiry using utf8mb4) ipb_expiry,
                    convert(ipb_range_start using utf8mb4) ipb_range_start,
                    convert(ipb_range_end using utf8mb4) ipb_range_end,
                    ipb_deleted,
                    ipb_block_email,
                    ipb_allow_usertalk,
                    ipb_parent_block_id,
                    ipb_by_actor,
                    ipb_reason_id

               from ipblocks
              where $CONDITIONS
                {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('ipb_timestamp', from_timestamp, to_timestamp)),
        'map-types': '"{}"'.format(','.join([
            'ipb_allow_usertalk=Boolean',
            'ipb_anon_only=Boolean',
            'ipb_auto=Boolean',
            'ipb_block_email=Boolean',
            'ipb_by=Long',
            'ipb_by_actor=Long',
            'ipb_by_text=String',
            'ipb_create_account=Boolean',
            'ipb_deleted=Boolean',
            'ipb_enable_autoblock=Boolean',
            'ipb_reason=String',
            'ipb_reason_id=Long',
        ])),
        'boundary-query': '''
            SELECT MIN(ipb_id),
                   MAX(ipb_id)
              FROM ipblocks
             WHERE TRUE
                 {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('ipb_timestamp', from_timestamp, to_timestamp)),
        'split-by': 'ipb_id',
        'mappers-weight': 0.0,
    }

    queries['ipblocks_restrictions'] = {
        'query': '''
             select ir_ipb_id,
                    ir_type,
                    ir_value

               from ipblocks_restrictions
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(ir_ipb_id), MAX(ir_ipb_id) FROM ipblocks_restrictions',
        'split-by': 'ir_ipb_id',
        'mappers-weight': 0.0,
    }

    queries['iwlinks'] = {
        'query': '''
             select iwl_from,
                    convert(iwl_prefix using utf8mb4) iwl_prefix,
                    convert(iwl_title using utf8mb4) iwl_title

               from iwlinks
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'iwl_from=Long',
            'iwl_prefix=String',
            'iwl_title=String',
        ])),
        'boundary-query': 'SELECT MIN(iwl_from), MAX(iwl_from) FROM iwlinks',
        'split-by': 'iwl_from',
        'mappers-weight': 0.5,
    }

    queries['langlinks'] = {
        'query': '''
             select ll_from,
                    convert(ll_lang using utf8mb4) ll_lang,
                    convert(ll_title using utf8mb4) ll_title

               from langlinks
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'll_from=Long',
            'll_lang=String',
            'll_title=String',
        ])),
        'boundary-query': 'SELECT MIN(ll_from), MAX(ll_from) FROM langlinks',
        'split-by': 'll_from',
        'mappers-weight': 0.5,
    }

    # NOTE: when updating the sanitization rule below,
    #   please also update the cloud replica logic that does the same thing:
    #   https://gerrit.wikimedia.org/g/operations/puppet/+/refs/heads/production/modules/profile/templates/wmcs/db/wikireplicas/maintain-views.yaml#566
    queries['linktarget'] = {
        'query': '''
             select lt_id,
                    lt_namespace,
                    convert(lt_title using utf8mb4) lt_title

               from linktarget
              where $CONDITIONS
                and (   exists(select 1 from templatelinks where tl_target_id = lt_id)
                     or exists(select 1 from pagelinks where pl_target_id = lt_id)
                    )
        ''',
        'map-types': '"{}"'.format(','.join([
            'lt_id=Long',
            'lt_namespace=Integer',
            'lt_title=String',
        ])),
        'boundary-query': 'SELECT MIN(lt_id), MAX(lt_id) FROM linktarget',
        'split-by': 'lt_id',
        'mappers-weight': 1.0,
    }

    queries['logging'] = {
        'query': '''
             select log_id,
                    convert(log_type using utf8mb4) log_type,
                    convert(log_action using utf8mb4) log_action,
                    convert(log_timestamp using utf8mb4) log_timestamp,
                    null log_user,
                    log_namespace,
                    convert(log_title using utf8mb4) log_title,
                    null log_comment,
                    convert(log_params using utf8mb4) log_params,
                    log_deleted,
                    null log_user_text,
                    log_page,
                    log_actor,
                    log_comment_id

               from logging
              where $CONDITIONS
                {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('log_timestamp', from_timestamp, to_timestamp)),
        'map-types': '"{}"'.format(','.join([
            'log_actor=Long',
            'log_comment=String',
            'log_comment_id=Long',
            'log_user=Long',
            'log_user_text=String',
        ])),
        'boundary-query': '''
            SELECT MIN(log_id),
                   MAX(log_id)
              FROM logging
             WHERE TRUE
                 {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('log_timestamp', from_timestamp, to_timestamp)),
        'split-by': 'log_id',
        'mappers-weight': 1.0,
    }

    queries['page'] = {
        'query': '''
             select page_id,
                    page_namespace,
                    convert(page_title using utf8mb4) page_title,
                    page_is_redirect,
                    page_is_new,
                    page_random,
                    convert(page_touched using utf8mb4) page_touched,
                    convert(page_links_updated using utf8mb4) page_links_updated,
                    page_latest,
                    page_len,
                    convert(page_content_model using utf8mb4) page_content_model

               from page
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'page_is_new=Boolean',
            'page_is_redirect=Boolean',
        ])),
        'boundary-query': 'SELECT MIN(page_id), MAX(page_id) FROM page',
        'split-by': 'page_id',
        'mappers-weight': 0.5,
    }

    queries['pagelinks'] = {
        'query': '''
             select pl_from,
                    pl_namespace,
                    convert(pl_title using utf8mb4) pl_title,
                    pl_from_namespace

               from pagelinks
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(pl_from), MAX(pl_from) FROM pagelinks',
        'split-by': 'pl_from',
        'mappers-weight': 1.0,
    }

    queries['page_props'] = {
        'query': '''
             select pp_page,
                    convert(pp_propname using utf8mb4) pp_propname,
                    convert(pp_value using utf8mb4) pp_value,
                    pp_sortkey

               from page_props
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'pp_page=Long',
            'pp_propname=String',
            'pp_value=String',
            'pp_sortkey=Float',
        ])),
        'boundary-query': 'SELECT MIN(pp_page), MAX(pp_page) FROM page_props',
        'split-by': 'pp_page',
        'mappers-weight': 0.125,
    }

    queries['page_restrictions'] = {
        'query': '''
             select pr_id,
                    pr_page,
                    convert(pr_type using utf8mb4) pr_type,
                    convert(pr_level using utf8mb4) pr_level,
                    pr_cascade,
                    convert(pr_expiry using utf8mb4) pr_expiry

               from page_restrictions
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'pr_id=Long',
            'pr_page=Long',
            'pr_type=String',
            'pr_level=String',
            'pr_cascade=Integer',
            'pr_expiry=String',
        ])),
        'boundary-query': 'SELECT MIN(pr_id), MAX(pr_id) FROM page_restrictions',
        'split-by': 'pr_id',
        'mappers-weight': 0.125,
    }

    queries['redirect'] = {
        'query': '''
             select rd_from,
                    rd_namespace,
                    convert(rd_title using utf8mb4) rd_title,
                    convert(rd_interwiki using utf8mb4) rd_interwiki,
                    convert(rd_fragment using utf8mb4) rd_fragment

               from redirect
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(rd_from), MAX(rd_from) FROM redirect',
        'split-by': 'rd_from',
        'mappers-weight': 0.125,
    }

    queries['revision'] = {
        'query': '''
             select rev_id,
                    rev_page,
                    null rev_text_id,
                    null rev_comment,
                    null rev_user,
                    null rev_user_text,
                    convert(rev_timestamp using utf8mb4) rev_timestamp,
                    rev_minor_edit,
                    rev_deleted,
                    rev_len,
                    rev_parent_id,
                    convert(rev_sha1 using utf8mb4) rev_sha1,
                    null rev_content_model,
                    null rev_content_format,
                    rev_actor,
                    rev_comment_id

               from revision
              where $CONDITIONS
                {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('rev_timestamp', from_timestamp, to_timestamp)),
        'map-types': '"{}"'.format(','.join([
            'rev_actor=Long',
            'rev_comment=String',
            'rev_comment_id=Long',
            'rev_deleted=Integer',
            'rev_minor_edit=Boolean',
            'rev_user=Long',
            'rev_user_text=String',
            'rev_text_id=Long',
            'rev_content_model=String',
            'rev_content_format=String',
        ])),
        'boundary-query': '''
            SELECT MIN(rev_id),
                   MAX(rev_id)
              FROM revision
             WHERE TRUE
                 {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('rev_timestamp', from_timestamp, to_timestamp)),
        'split-by': 'rev_id',
        'mappers-weight': 1.0,
    }

    queries['slots'] = {
        'query': '''
             select slot_revision_id,
                    slot_role_id,
                    slot_content_id,
                    slot_origin

               from slots
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(slot_revision_id), MAX(slot_revision_id) FROM slots',
        'split-by': 'slot_revision_id',
        'mappers-weight': 1.0,
    }

    queries['slot_roles'] = {
        'query': '''
             select role_id,
                    convert(role_name using utf8mb4) role_name

               from slot_roles
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(role_id), MAX(role_id) FROM slot_roles',
        'split-by': 'role_id',
        'mappers-weight': 0.0,
    }

    queries['templatelinks'] = {
        'query': '''
             select tl_from,
                    tl_from_namespace,
                    null as tl_namespace,
                    null as tl_title,
                    tl_target_id

               from templatelinks
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'tl_from=Long',
            'tl_from_namespace=Integer',
            'tl_namespace=Integer',
            'tl_title=String',
            'tl_target_id=Long',
        ])),
        'boundary-query': 'SELECT MIN(tl_from), MAX(tl_from) FROM templatelinks',
        'split-by': 'tl_from',
        'mappers-weight': 1.0,
    }

    queries['user'] = {
        'query': '''
             select user_id,
                    convert(user_name using utf8mb4) user_name,
                    user_name user_name_binary,
                    convert(user_real_name using utf8mb4) user_real_name,
                    convert(user_email using utf8mb4) user_email,
                    convert(user_touched using utf8mb4) user_touched,
                    convert(user_registration using utf8mb4) user_registration,
                    user_editcount,
                    convert(user_password_expires using utf8mb4) user_password_expires

               from user
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'user_id=Long',
            'user_editcount=Long',
        ])),
        'boundary-query': 'SELECT MIN(user_id), MAX(user_id) FROM user',
        'split-by': 'user_id',
        'mappers-weight': 0.5,
    }

    queries['user_groups'] = {
        'query': '''
             select ug_user,
                    convert(ug_group using utf8mb4) ug_group

               from user_groups
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(ug_user), MAX(ug_user) FROM user_groups',
        'split-by': 'ug_user',
        'mappers-weight': 0.0,
    }

    queries['user_properties'] = {
        'query': '''
             select up_user,
                    convert(up_property using utf8mb4) up_property,
                    convert(up_value using utf8mb4) up_value

               from user_properties
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'up_user=Long',
            'up_property=String',
            'up_value=String',
        ])),
        'boundary-query': 'SELECT MIN(up_user), MAX(up_user) FROM user_properties',
        'split-by': 'up_user',
        'mappers-weight': 0.125,
    }

    wbc_entity_usage_sqoopable_dbs = get_dbnames_from_mw_config(['wikidataclient.dblist'])
    # Manually removed table (empty in prod, not replicated in labs)
    wbc_entity_usage_sqoopable_dbs.discard('sewikimedia')

    queries['wbc_entity_usage'] = {
        'query': '''
             select eu_row_id,
                    convert(eu_entity_id using utf8mb4) eu_entity_id,
                    convert(eu_aspect using utf8mb4) eu_aspect,
                    eu_page_id

               from wbc_entity_usage
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(eu_row_id), MAX(eu_row_id) FROM wbc_entity_usage',
        'split-by': 'eu_row_id',
        'map-types': '"{}"'.format(','.join([
            'eu_row_id=Long',
            'eu_entity_id=String',
            'eu_aspect=String',
            'eu_page_id=Long'
        ])),
        'mappers-weight': 1.0,
        'sqoopable_dbnames': wbc_entity_usage_sqoopable_dbs,
    }

    ############################################################################
    # Tables sqooped from production replica
    #   cu_changes and watchlist are not available in labs
    #   actor and comments are too slow due to expensive join at sanitization
    ############################################################################

    # documented at https://www.mediawiki.org/wiki/Extension:CheckUser/cu_changes_table
    queries['cu_changes'] = {
        'query': '''
             select cuc_id,
                    cuc_namespace,
                    cuc_title,
                    coalesce(actor_user, 0) cuc_user,
                    convert(actor_name using utf8mb4) cuc_user_text,
                    cuc_actor,
                    cuc_actiontext,
                    convert(comment_text using utf8mb4) cuc_comment,
                    cuc_comment_id,
                    cuc_minor,
                    cuc_page_id,
                    cuc_this_oldid,
                    cuc_last_oldid,
                    cuc_type,
                    convert(cuc_timestamp using utf8mb4) cuc_timestamp,
                    convert(cuc_ip using utf8mb4) cuc_ip,
                    convert(cuc_agent using utf8mb4) cuc_agent
               from cu_changes
                        inner join
                    actor           on actor_id = cuc_actor
                        inner join
                    comment         on comment_id = cuc_comment_id
              where $CONDITIONS
                {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('cuc_timestamp', from_timestamp, to_timestamp)),
        'map-types': '"{}"'.format(','.join([
            'cuc_id=Long',
            'cuc_namespace=Integer',
            'cuc_title=String',
            'cuc_user=Long',
            'cuc_user_text=String',
            'cuc_actor=Long',
            'cuc_actiontext=String',
            'cuc_comment=String',
            'cuc_comment_id=Long',
            'cuc_minor=Boolean',
            'cuc_page_id=Long',
            'cuc_this_oldid=Long',
            'cuc_last_oldid=Long',
            'cuc_type=Integer',
            'cuc_timestamp=String',
            'cuc_ip=String',
            'cuc_agent=String',
        ])),
        'boundary-query': '''
            SELECT MIN(cuc_id),
                   MAX(cuc_id)
              FROM cu_changes
             WHERE TRUE
                 {ts_clause}
        '''.format(ts_clause=make_timestamp_clause('cuc_timestamp', from_timestamp, to_timestamp)),
        'split-by': 'cuc_id',
        'mappers-weight': 0.5,
    }

    queries['actor'] = {
        # NOTE: we don't need actor_user, as tables key into here via actor_id just to get the
        # actor_name.  But it seems like a good idea to have it for other purposes and joins
        'query': '''
             select actor_id,
                    actor_user,
                    convert(actor_name using utf8mb4) actor_name
               from actor
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(actor_id), MAX(actor_id) FROM actor',
        'split-by': 'actor_id',
        'mappers-weight': 0.5,
    }

    queries['comment'] = {
        # NOTE: skipping comment_hash and comment_data, not needed
        'query': '''
             select comment_id,
                    convert(comment_text using utf8mb4) comment_text
               from comment
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(comment_id), MAX(comment_id) FROM comment',
        'split-by': 'comment_id',
        'mappers-weight': 1.0,
    }

    queries['discussiontools_subscription'] = {
        'query': '''
             select sub_id,
                    convert(sub_item using utf8mb4) sub_item,
                    sub_namespace,
                    convert(sub_title using utf8mb4) sub_title,
                    convert(sub_section using utf8mb4) sub_section,
                    sub_state,
                    sub_user,
                    convert(sub_created using utf8mb4) sub_created,
                    convert(sub_notified using utf8mb4) sub_notified

               from discussiontools_subscription
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'sub_id=Long',
            'sub_item=String',
            'sub_namespace=Integer',
            'sub_title=String',
            'sub_section=String',
            'sub_state=Integer',
            'sub_user=Long',
            'sub_created=String',
            'sub_notified=String',
        ])),
        'boundary-query': 'SELECT MIN(sub_id), MAX(sub_id) FROM discussiontools_subscription',
        'split-by': 'sub_id',
        'mappers-weight': 1.0,
    }

    queries['wikilambda_zobject_labels'] = {
        'query': '''
             select wlzl_id,
                    convert(wlzl_zobject_zid using utf8mb4) wlzl_zobject_zid,
                    convert(wlzl_type using utf8mb4) wlzl_type,
                    convert(wlzl_language using utf8mb4) wlzl_language,
                    wlzl_label_primary,
                    convert(wlzl_return_type using utf8mb4) wlzl_return_type

               from wikilambda_zobject_labels
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'wlzl_id=Long',
            'wlzl_zobject_zid=String',
            'wlzl_type=String',
            'wlzl_language=String',
            'wlzl_label_primary=Boolean',
            'wlzl_return_type=String'
        ])),
        'boundary-query': 'SELECT MIN(wlzl_id), MAX(wlzl_id) FROM wikilambda_zobject_labels',
        'split-by': 'wlzl_id',
        'mappers-weight': 1.0,
    }

    queries['wikilambda_zobject_function_join'] = {
        'query': '''
             select wlzf_id,
                    convert(wlzf_ref_zid using utf8mb4) wlzf_ref_zid,
                    convert(wlzf_zfunction_zid using utf8mb4) wlzf_zfunction_zid,
                    convert(wlzf_type using utf8mb4) wlzf_type

               from wikilambda_zobject_function_join
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'wlzf_id=Long',
            'wlzf_ref_zid=String',
            'wlzf_zfunction_zid=String',
            'wlzf_type=String'
        ])),
        'boundary-query': 'SELECT MIN(wlzf_id), MAX(wlzf_id) FROM wikilambda_zobject_function_join',
        'split-by': 'wlzf_id',
        'mappers-weight': 1.0,
    }

    queries['watchlist'] = {
        'query': '''
             select wl_id,
                    wl_user,
                    wl_namespace,
                    convert(wl_title using utf8mb4) wl_title,
                    convert(wl_notificationtimestamp using utf8mb4) wl_notificationtimestamp

               from watchlist
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'wl_id=Long',
            'wl_user=Long',
            'wl_namespace=Integer',
            'wl_title=String',
            'wl_notificationtimestamp=String',
        ])),
        'boundary-query': 'SELECT MIN(wl_id), MAX(wl_id) FROM watchlist',
        'split-by': 'wl_id',
        'mappers-weight': 1.0,
    }


    ############################################################################
    # Tables sqooped from wikibase (wikidatawiki only)
    ############################################################################

    queries['wbt_item_terms'] = {
        'query': '''
             select wbit_id,
                    wbit_item_id,
                    wbit_term_in_lang_id
               from wbt_item_terms
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'wbit_id=Long',
            'wbit_item_id=Long',
            'wbit_term_in_lang_id=Long',
        ])),
        'boundary-query': 'SELECT MIN(wbit_id), MAX(wbit_id) FROM wbt_item_terms',
        'split-by': 'wbit_id',
        'mappers-weight': 1.0,
        'sqoopable_dbnames': ['wikidatawiki'],
    }

    queries['wbt_property_terms'] = {
        'query': '''
             select wbpt_id,
                    wbpt_property_id,
                    wbpt_term_in_lang_id
               from wbt_property_terms
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'wbpt_id=Long',
            'wbpt_property_id=Long',
            'wbpt_term_in_lang_id=Long',
        ])),
        'boundary-query': 'SELECT MIN(wbpt_id), MAX(wbpt_id) FROM wbt_property_terms',
        'split-by': 'wbpt_id',
        'mappers-weight': 0.5,
        'sqoopable_dbnames': ['wikidatawiki'],
    }

    queries['wbt_term_in_lang'] = {
        'query': '''
             select wbtl_id,
                    wbtl_type_id,
                    wbtl_text_in_lang_id
               from wbt_term_in_lang
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'wbtl_id=Long',
            'wbtl_type_id=Integer',
            'wbtl_text_in_lang_id=Long',
        ])),
        'boundary-query': 'SELECT MIN(wbtl_id), MAX(wbtl_id) FROM wbt_term_in_lang',
        'split-by': 'wbtl_id',
        'mappers-weight': 1.0,
        'sqoopable_dbnames': ['wikidatawiki'],
    }

    queries['wbt_text'] = {
        'query': '''
             select wbx_id,
                    convert(wbx_text using utf8mb4) wbx_text
               from wbt_text
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'wbx_id=Long',
            'wbx_text=String',
        ])),
        'boundary-query': 'SELECT MIN(wbx_id), MAX(wbx_id) FROM wbt_text',
        'split-by': 'wbx_id',
        'mappers-weight': 1.0,
        'sqoopable_dbnames': ['wikidatawiki'],
    }

    queries['wbt_text_in_lang'] = {
        'query': '''
             select wbxl_id,
                    convert(wbxl_language using utf8mb4) wbxl_language,
                    wbxl_text_id
               from wbt_text_in_lang
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'wbxl_id=Long',
            'wbxl_text_id=Long',
            'wbxl_language=String',
        ])),
        'boundary-query': 'SELECT MIN(wbxl_id), MAX(wbxl_id) FROM wbt_text_in_lang',
        'split-by': 'wbxl_id',
        'mappers-weight': 1.0,
        'sqoopable_dbnames': ['wikidatawiki'],
    }

    queries['wbt_type'] = {
        'query': '''
             select wby_id,
                    convert(wby_name using utf8mb4) wby_name
               from wbt_type
              where $CONDITIONS
        ''',
        'boundary-query': 'SELECT MIN(wby_id), MAX(wby_id) FROM wbt_type',
        'split-by': 'wby_id',
        'mappers-weight': 0,
        'sqoopable_dbnames': ['wikidatawiki'],
    }

    if filter_tables:
        filter_tables_dict = {t: True for t in filter_tables}
        if len(set(filter_tables_dict.keys()) - set(queries.keys())):
            logger.error('Bad list of tables to export: {}'.format(filter_tables))
            sys.exit(1)
        return {k: v for k, v in queries.items() if k in filter_tables_dict}
    else:
        return queries


def table_path_to_tmp_path(table_path, tmp_base_path):
    return tmp_base_path + re.sub('[^a-zA-Z0-9/]+', '', table_path)


def check_already_running_or_exit(yarn_job_name_prefix):
    # This works since the check doesn't involve 'full word' matching
    if is_yarn_application_running(yarn_job_name_prefix):
        logger.warn('{} is already running, exiting.'.format(yarn_job_name_prefix))
        sys.exit(1)


def check_hdfs_path_or_exit(tables, table_path_template, tmp_base_path, force, dry_run):
    safe = True
    logger.info('Checking HDFS paths')
    for table in tables:
        table_path = table_path_template.format(table=table)
        tmp_table_path = table_path_to_tmp_path(table_path, tmp_base_path)

        # Delete temporary folder if it exists in any case
        if Hdfs.ls(tmp_table_path, include_children=False):
            if not dry_run:
                Hdfs.rm(tmp_table_path)
            logger.info('temporary path {} deleted from HDFS.'.format(tmp_table_path))

        # Check if real folder exist and delee it if --force flag is on
        if Hdfs.ls(table_path, include_children=False):
            if force:
                if not dry_run:
                    Hdfs.rm(table_path)
                logger.info('Forcing: {} deleted from HDFS.'.format(table_path))
            else:
                logger.error('{} already exists in HDFS.'.format(table_path))
                safe = False

    if not safe:
        sys.exit(1)
