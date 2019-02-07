"""
Wikimedia Anaytics Refinery sqoop python helpers
"""
import sys
import logging
import re

from subprocess import check_call, DEVNULL
from refinery.util import is_yarn_application_running, HdfsUtils

logger = logging.getLogger()


class SqoopConfig:

    def __init__(self, yarn_job_name_prefix,
                 user, password_file, jdbc_host,
                 num_mappers, output_format, tmp_base_path,
                 table_path_template, dbname, dbpostfix, table,
                 query, split_by, map_types,
                 generate_jar, jar_file,
                 current_try, dry_run):

        self.yarn_job_name_prefix = yarn_job_name_prefix
        self.user = user
        self.password_file = password_file
        self.jdbc_host = jdbc_host
        self.num_mappers = num_mappers
        self.output_format = output_format
        self.tmp_base_path = tmp_base_path
        self.table_path_template = table_path_template
        self.dbname = dbname
        self.dbpostfix = dbpostfix
        self.table = table
        self.query = query
        self.split_by = split_by
        self.map_types = map_types
        self.generate_jar = generate_jar
        self.jar_file = jar_file
        self.current_try = current_try
        self.dry_run = dry_run

    def __str__(self):
        return self.dbname + ':' + self.table


def sqoop_wiki(config):
    """
    Imports a pre-determined list of tables from dbname

    Parameters
        config: SqoopConfig object filed in with needed parameters

    Returns
        True if the sqoop worked
        False if the sqoop errored or failed in any way
    """
    full_table = '.'.join([config.dbname, config.table])
    log_message = '{} (try {})'.format(full_table, config.current_try)
    logger.info('STARTING: {}'.format(log_message))
    try:
        query = config.query
        command = 'import'
        if config.generate_jar:
            query = query + ' and 1=0'
            command = 'codegen'

        sqoop_arguments = [
            'sqoop',
            command,
            '-D'                , "mapred.job.name='{}-{}'".format(
                config.yarn_job_name_prefix, full_table),
            '--username'        , config.user,
            '--password-file'   , config.password_file,
            '--connect'         , config.jdbc_host + config.dbname + config.dbpostfix,
            '--query'           , config.query,
        ]

        if config.generate_jar:
            sqoop_arguments += [
                '--class-name'      , config.table,
                '--outdir'          , config.generate_jar,
                '--bindir'          , config.generate_jar,
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
                '--num-mappers'     , str(config.num_mappers),
                '--as-{}file'.format(config.output_format),
            ]
            if config.num_mappers > 1:
                sqoop_arguments += [
                    '--split-by'    , config.split_by,
                ]

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
        if not config.generate_jar:
            logger.info('Deleting temporary target directory {} if it exists'.format(tmp_target_directory))
            if not config.dry_run:
                try:
                    HdfsUtils.rm(tmp_target_directory)
                except(Exception):
                    pass

        logger.info('Sqooping with: {}'.format(sqoop_arguments))
        logger.debug('You can copy the parameters above and execute the sqoop command manually')
        # Ignore sqoop output because it's in Yarn and grabbing output is way complicated
        if not config.dry_run:
            check_call(sqoop_arguments, stdout=DEVNULL, stderr=DEVNULL)
        if not config.generate_jar:
            logger.info('Moving sqooped forlder from {} to {}'.format(tmp_target_directory, target_directory))
            if not config.dry_run:
                HdfsUtils.mv(tmp_target_directory, target_directory, inParent=False)
        logger.info('FINISHED: {}'.format(log_message))
        return None
    except(Exception):
        logger.exception('ERROR: {}'.format(log_message))
        config.current_try += 1
        return config


def validate_tables_and_get_queries(filter_tables, from_timestamp, to_timestamp, labsdb):
    """
    Returns a dictionary of mediawiki table names to the correct sqoop
    query to execute for import.

    Notes
        convert(... using utf8) is used to decode varbinary fields into strings
        map-types is used to handle some databases having booleans in
          tinyint(1) and others in tinyint(3,4) (newer databases like wikivoyage)

    Parameters
        filter_tables: list of tables to return queries for, None for all tables
        from_timestamp: imported timestamps must be newer than this, (YYYYMMDDHHmmss)
        to_timestamp: timestamps must be *strictly* older than this, (YYYYMMDDHHmmss)
        labsdb: True or False, whether _p should be appended to table names

    Returns
        An object of the form:
            {
                'table-name': {
                    'query'     : <<the sql query to run on mysql>>,
                    'map-types' : <<any info to pass as --map-column-java>>,
                    'split-by'  : <<the column to use to parallelize imports>>,
                },
                ...
            }
    """
    queries = {}

    queries['archive'] = {
        'query': '''
             select ar_id,
                    ar_namespace,
                    convert(ar_title using utf8) ar_title,
                    convert('' using utf8) ar_text,
                    convert(ar_comment using utf8) ar_comment,
                    ar_user,
                    convert(ar_user_text using utf8) ar_user_text,
                    convert(ar_timestamp using utf8) ar_timestamp,
                    ar_minor_edit,
                    convert('' using utf8) ar_flags,
                    ar_rev_id,
                    ar_text_id,
                    ar_deleted,
                    ar_len,
                    ar_page_id,
                    ar_parent_id,
                    convert(ar_sha1 using utf8) ar_sha1,
                    convert('' using utf8) ar_content_model,
                    convert('' using utf8) ar_content_format,
                    ar_actor,
                    coalesce(ar_comment_id, 0) ar_comment_id

               from archive
              where $CONDITIONS
                and ar_timestamp >= '{f}'
                and ar_timestamp <  '{t}'
        '''.format(model="''" if labsdb else 'ar_content_model',
                   format="''" if labsdb else 'ar_content_format',
                   f=from_timestamp, t=to_timestamp),
        'map-types': '"{}"'.format(','.join([
            'ar_minor_edit=Boolean',
            'ar_deleted=Integer',
            'ar_actor=Long',
            'ar_comment_id=Long',
        ])),

        'split-by': 'ar_id',
    }

    queries['change_tag'] = {
        'query': '''
             select ct_id,
                    ct_log_id,
                    ct_rev_id,
                    convert(ct_tag using utf8) ct_tag,
                    ct_tag_id,
                    convert(ct_params using utf8) ct_params

               from change_tag
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'ct_id=Long',
            'ct_log_id=Long',
            'ct_rev_id=Long',
            'ct_tag_id=Long',
        ])),

        'split-by': 'ct_id',
    }

    queries['change_tag_def'] = {
        'query': '''
             select ctd_id,
                    convert(ctd_name using utf8) ctd_name,
                    ctd_user_defined,
                    ctd_count

               from change_tag_def
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'ctd_id=Long',
            'ctd_user_defined=Boolean',
            'ctd_count=Long'
        ])),

        'split-by': 'ctd_id',
    }

    queries['ipblocks'] = {
        'query': '''
             select ipb_id,
                    convert(ipb_address using utf8) ipb_address,
                    ipb_user,
                    ipb_by,
                    convert(ipb_by_text using utf8) ipb_by_text,
                    convert(ipb_reason using utf8) ipb_reason,
                    convert(ipb_timestamp using utf8) ipb_timestamp,
                    ipb_auto,
                    ipb_anon_only,
                    ipb_create_account,
                    ipb_enable_autoblock,
                    convert(ipb_expiry using utf8) ipb_expiry,
                    convert(ipb_range_start using utf8) ipb_range_start,
                    convert(ipb_range_end using utf8) ipb_range_end,
                    ipb_deleted,
                    ipb_block_email,
                    ipb_allow_usertalk,
                    ipb_parent_block_id,
                    ipb_by_actor,
                    ipb_reason_id

               from ipblocks
              where $CONDITIONS
                and ipb_timestamp >= '{f}'
                and ipb_timestamp <  '{t}'
        '''.format(f=from_timestamp, t=to_timestamp),
        'map-types': '"{}"'.format(','.join([
            'ipb_auto=Boolean',
            'ipb_anon_only=Boolean',
            'ipb_create_account=Boolean',
            'ipb_enable_autoblock=Boolean',
            'ipb_deleted=Boolean',
            'ipb_block_email=Boolean',
            'ipb_allow_usertalk=Boolean',
            'ipb_by_actor=Long',
            'ipb_reason_id=Long',
        ])),

        'split-by': 'ipb_id',
    }

    queries['logging'] = {
        'query': '''
             select log_id,
                    convert(log_type using utf8) log_type,
                    convert(log_action using utf8) log_action,
                    convert(log_timestamp using utf8) log_timestamp,
                    log_user,
                    log_namespace,
                    convert(log_title using utf8) log_title,
                    convert(log_comment using utf8) log_comment,
                    convert(log_params using utf8) log_params,
                    log_deleted,
                    convert(log_user_text using utf8) log_user_text,
                    log_page,
                    log_actor,
                    log_comment_id

               from logging_compat
              where $CONDITIONS
                and log_timestamp >= '{f}'
                and log_timestamp <  '{t}'
        '''.format(t=to_timestamp, f=from_timestamp),
        'map-types': '"{}"'.format(','.join([
            'log_user=Long',
            'log_actor=Long',
            'log_comment_id=Long',
        ])),
        'split-by': 'log_id',
    }

    queries['page'] = {
        'query': '''
             select page_id,
                    page_namespace,
                    convert(page_title using utf8) page_title,
                    convert(page_restrictions using utf8) page_restrictions,
                    page_is_redirect,
                    page_is_new,
                    page_random,
                    convert(page_touched using utf8) page_touched,
                    convert(page_links_updated using utf8) page_links_updated,
                    page_latest,
                    page_len,
                    convert(page_content_model using utf8) page_content_model

               from page
              where $CONDITIONS
        ''',
        'map-types': '"{}"'.format(','.join([
            'page_is_redirect=Boolean',
            'page_is_new=Boolean',
        ])),

        'split-by': 'page_id',
    }

    queries['pagelinks'] = {
        'query': '''
             select pl_from,
                    pl_namespace,
                    convert(pl_title using utf8) pl_title,
                    pl_from_namespace

               from pagelinks
              where $CONDITIONS
        ''',

        'split-by': 'pl_from',
    }

    queries['redirect'] = {
        'query': '''
             select rd_from,
                    rd_namespace,
                    convert(rd_title using utf8) rd_title,
                    convert(rd_interwiki using utf8) rd_interwiki,
                    convert(rd_fragment using utf8) rd_fragment

               from redirect
              where $CONDITIONS
        ''',

        'split-by': 'rd_from',
    }

    queries['revision'] = {
        'query': '''
             select rev_id,
                    rev_page,
                    rev_text_id,
                    convert(rev_comment using utf8) rev_comment,
                    rev_user,
                    convert(rev_user_text using utf8) rev_user_text,
                    convert(rev_timestamp using utf8) rev_timestamp,
                    rev_minor_edit,
                    rev_deleted,
                    rev_len,
                    rev_parent_id,
                    convert(rev_sha1 using utf8) rev_sha1,
                    convert(rev_content_model using utf8) rev_content_model,
                    convert(rev_content_format using utf8) rev_content_format
                    {labsdb}

               from revision
              where $CONDITIONS
                and rev_timestamp >= '{f}'
                and rev_timestamp <  '{t}'
        '''.format(
            f=from_timestamp,
            t=to_timestamp,
            labsdb=',rev_actor,coalesce(rev_comment_id,0) rev_comment_id' if labsdb else ',null rev_actor,null rev_comment_id'),
        'map-types': '"{}"'.format(','.join([
            'rev_minor_edit=Boolean',
            'rev_deleted=Integer',
            'rev_actor=Long',
            'rev_comment_id=Long',
        ])),

        'split-by': 'rev_id',
    }

    queries['user'] = {
        'query': '''
             select user_id,
                    convert(user_name using utf8) user_name,
                    user_name user_name_binary,
                    convert(user_real_name using utf8) user_real_name,
                    convert(user_email using utf8) user_email,
                    convert(user_touched using utf8) user_touched,
                    convert(user_registration using utf8) user_registration,
                    user_editcount,
                    convert(user_password_expires using utf8) user_password_expires

               from user
              where $CONDITIONS
        ''',

        'split-by': 'user_id',
    }

    queries['user_groups'] = {
        'query': '''
             select ug_user,
                    convert(ug_group using utf8) ug_group

               from user_groups
              where $CONDITIONS
        ''',

        'split-by': 'ug_user',
    }

    queries['change_tag'] = {
        'query': '''
             select ct_rev_id,
                    ct_tag_id,
                    ctd_name
               from change_tag
                        inner join
                    change_tag_def  on ct_tag_id = ctd_id
              where $CONDITIONS
        ''',

        'split-by': 'ct_rev_id',
    }

    # documented at https://www.mediawiki.org/wiki/Extension:CheckUser/cu_changes_table
    queries['cu_changes'] = {
        'query': '''
             select cuc_id,
                    cuc_namespace,
                    cuc_title,
                    cuc_user,
                    convert(cuc_user_text using utf8) cuc_user_text,
                    cuc_actiontext,
                    convert(cuc_comment using utf8) cuc_comment,
                    cuc_minor,
                    cuc_page_id,
                    cuc_this_oldid,
                    cuc_last_oldid,
                    cuc_type,
                    convert(cuc_timestamp using utf8) cuc_timestamp,
                    cuc_ip,
                    cuc_agent
               from cu_changes
              where $CONDITIONS
                and cuc_timestamp >= '{f}'
                and cuc_timestamp <  '{t}'
        '''.format(f=from_timestamp, t=to_timestamp),
        'map-types': '"{}"'.format(','.join([
            'cuc_minor=Boolean',
        ])),
        'split-by': 'cuc_id',
    }

    queries['actor'] = {
        # NOTE: we don't need actor_user, as tables key into here via actor_id just to get the
        # actor_name.  But it seems like a good idea to have it for other purposes and joins
        'query': '''
             select actor_id,
                    actor_user,
                    convert(actor_name using utf8) actor_name
               from actor
              where $CONDITIONS
        ''',
        'split-by': 'actor_id',
    }

    queries['comment'] = {
        # NOTE: skipping comment_hash and comment_data, not needed
        'query': '''
             select comment_id,
                    convert(comment_text using utf8) comment_text
               from comment
              where $CONDITIONS
        ''',
        'split-by': 'comment_id',
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
        if HdfsUtils.ls(tmp_table_path, include_children=False):
            if not dry_run:
                HdfsUtils.rm(tmp_table_path)
            logger.info('temporary path {} deleted from HDFS.'.format(tmp_table_path))

        # Check if real folder exist and delee it if --force flag is on
        if HdfsUtils.ls(table_path, include_children=False):
            if force:
                if not dry_run:
                    HdfsUtils.rm(table_path)
                logger.info('Forcing: {} deleted from HDFS.'.format(table_path))
            else:
                logger.error('{} already exists in HDFS.'.format(table_path))
                safe = False

    if not safe:
        sys.exit(1)
