-- Refine Webrequest
-- #################
--
-- Create a partition in the Hive table wmf.webrequest (as parquet) from the Hive table raw.webrequest (JSON).
-- This dataset describes a view of the traffic arriving to the servers of the foundation. See more details on DataHub.
--
-- Parameters:
--   refinery_jar                 -- path to the jar to import for UDFs (HDFS path or local path).
--   source_table                 -- Fully qualified table name to compute the statistics for.
--   destination_table            -- Fully qualified table name to store the computed statistics in. This table should have the
--                                   schema described in the first sub-request.
--   webrequest_source            -- webrequest_source of partition to compute statistics for. (text, upload, or test_text)
--   record_version               -- record_version keeping track of changes in the table content definition.
--                                  (See more details on Wikitech https//w.wiki/6Qpg)
--   coalesce_partitions          -- number of files in the output partition can't exceed it.
--   spark_sql_shuffle_partitions -- The number of partitions to use when computing
--   excluded_row_ids             -- A list of rows to remove, defined from their hostname and sequence values, formatted as
--                                   "'hostname1,sequence1','hostname2,sequence2'", or empty-string if no row is to be removed.
--                                   This is to be used when a small number of rows has incorrect formatting for instance.
--                                   More doc here: https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/Webrequest#Pipeline_Administration
--   year                         -- year of partition to compute statistics for.
--   month                        -- month of partition to compute statistics for.
--   day                          -- day of partition to compute statistics for.
--   hour                         -- hour of partition to compute statistics for.
--
-- Usage example:
--     spark3-sql \
--         --master yarn \
--         --executor-memory 12G \
--         --executor-cores 2 \
--         --driver-memory 4G \
--         --driver-cores 1 \
--         --conf spark.dynamicAllocation.maxExecutors=128 \
--         --name test-refine-webrequest-hourly \
--         -f refine_webrequest_hourly.hql \
--         -d refinery_jar=hdfs:///wmf/refinery/current/artifacts/org/wikimedia/analytics/refinery/refinery-hive-0.2.13-shaded.jar \
--         -d source_table=wmf_raw.webrequest \
--         -d destination_table=my_user.webrequest \
--         -d webrequest_source=text \
--         -d record_version=0.0.1 \
--         -d coalesce_partitions=256 \
--         -d spark_sql_shuffle_partitions=256 \
--         -d excluded_row_ids= \
--         -d year=2022 \
--         -d month=12 \
--         -d day=30 \
--         -d hour=1


ADD JAR ${refinery_jar};
CREATE TEMPORARY FUNCTION is_pageview as 'org.wikimedia.analytics.refinery.hive.IsPageviewUDF';
CREATE TEMPORARY FUNCTION geocoded_data as 'org.wikimedia.analytics.refinery.hive.GetGeoDataUDF';
CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.GetUAPropertiesUDF';
CREATE TEMPORARY FUNCTION get_access_method as 'org.wikimedia.analytics.refinery.hive.GetAccessMethodUDF';
CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
CREATE TEMPORARY FUNCTION referer_classify AS 'org.wikimedia.analytics.refinery.hive.GetRefererTypeUDF';
CREATE TEMPORARY FUNCTION get_pageview_info AS 'org.wikimedia.analytics.refinery.hive.GetPageviewInfoUDF';
CREATE TEMPORARY FUNCTION normalize_host AS 'org.wikimedia.analytics.refinery.hive.GetHostPropertiesUDF';
CREATE TEMPORARY FUNCTION get_tags AS 'org.wikimedia.analytics.refinery.hive.GetWebrequestTagsUDF';
CREATE TEMPORARY FUNCTION isp_data as 'org.wikimedia.analytics.refinery.hive.GetISPDataUDF';
CREATE TEMPORARY FUNCTION get_referer_data as 'org.wikimedia.analytics.refinery.hive.GetRefererDataUDF';

-- We set spark.sql.mapKeyDedupPolicy to LAST_WIN to prevent duplicate map keys
-- in str_to_map() calls to break the query. See: https://phabricator.wikimedia.org/T351909
SET spark.sql.mapKeyDedupPolicy = LAST_WIN;
SET spark.sql.shuffle.partitions = ${spark_sql_shuffle_partitions};

-- The distinct_rows CTE provides DISTINCT on raw data only. This prevents augmented fields to be shuffled, therefore
-- reduces IO cost significantly. NB: This is feasible as augmented values are deterministically computed.
--
-- The distinct_rows_and_reused_fields CTE materializes reused fields in the reduce step, then preventing computation
-- at every reuse.
--
-- Finally the not-reused fields are computed. And the data is written as parquet to the partition folder.
--
-- When adding new fields:
--  * fields imported from the wmf_raw.webrequest table need to be included in the two CTEs and the main SELECT
--  * fields computed from fields already present in the distinct_rows CTE and reused multiple times in the main select
--    need to be added to the distinct_rows_and_reused_fields CTE, to be reused in the main SELECT
--  * fields computed from fields already present in any CTE and used a single time in the main select need to be added
--    to the main SELECT only

WITH excluded_rows AS (
    SELECT
        substr(row_id, 0, locate(',', row_id) - 1) AS excluded_hostname,
        cast(substr(row_id, locate(',', row_id) + 1, length(row_id)) AS BIGINT) AS excluded_sequence
    FROM (
        SELECT explode(array(${excluded_row_ids})) AS row_id
    )
),

distinct_rows AS (

    SELECT /*+ BROADCAST(excluded_rows) */ DISTINCT
        hostname,
        sequence,
        dt,
        time_firstbyte,
        ip,
        cache_status,
        http_status,
        response_size,
        http_method,
        uri_host,
        uri_path,
        uri_query,
        content_type,
        referer,
        x_forwarded_for,
        user_agent,
        accept_language,
        x_analytics,
        `range`,
        x_cache,
        accept,
        tls,
        ch_ua,
        ch_ua_mobile,
        ch_ua_platform,
        ch_ua_arch,
        ch_ua_bitness,
        ch_ua_full_version_list,
        ch_ua_model,
        ch_ua_platform_version
    FROM
        ${source_table}
    LEFT ANTI JOIN excluded_rows
      ON hostname = excluded_hostname AND sequence = excluded_sequence
    WHERE
        webrequest_source='${webrequest_source}' AND
        year=${year} AND month=${month} AND day=${day} AND hour=${hour}

), distinct_rows_and_reused_fields AS (

     SELECT
         distinct_rows.*,
         -- Materialize reused computed fields
         is_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics) as is_pageview,
         ua_parser(user_agent) as user_agent_map,
         CASE COALESCE(x_analytics, '-')
             WHEN '-' THEN NULL
             ELSE str_to_map(x_analytics, '\;', '=')
             END as x_analytics_map
     FROM distinct_rows

)

INSERT OVERWRITE TABLE ${destination_table}
PARTITION(webrequest_source='${webrequest_source}', year=${year}, month=${month}, day=${day}, hour=${hour})
SELECT /*+ COALESCE(${coalesce_partitions}) */
    hostname,
    sequence,
    dt,
    time_firstbyte,
    ip,
    cache_status,
    http_status,
    response_size,
    http_method,
    uri_host,
    uri_path,
    uri_query,
    content_type,
    referer,
    x_forwarded_for,
    user_agent,
    accept_language,
    x_analytics,
    `range`,
    is_pageview,
    '${record_version}' as record_version,
    ip as client_ip,  -- client_ip is deprecated
    geocoded_data(ip) as geocoded_data,
    x_cache,
    user_agent_map,
    x_analytics_map,
    CAST(unix_timestamp(dt, "yyyy-MM-dd'T'HH:mm:ssX") as timestamp) as ts,
    get_access_method(uri_host, user_agent) as access_method,
    CASE
        WHEN ((user_agent_map['device_family'] = 'Spider') OR (is_spider(user_agent))) THEN 'spider'
        ELSE 'user'
        END as agent_type,
    NULL as is_zero,
    referer_classify(referer) as referer_class,
    normalize_host(uri_host) as normalized_host,
    CASE
        WHEN is_pageview THEN get_pageview_info(uri_host, uri_path, uri_query)
        ELSE NULL
        END as pageview_info,
    CAST(x_analytics_map['page_id'] AS BIGINT) as page_id,
    CAST(x_analytics_map['ns'] AS BIGINT) as namespace_id,
    get_tags(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics) as tags,
    isp_data(ip) as isp_data,
    accept,
    tls,
    CASE COALESCE(tls, '-')
        WHEN '-' THEN NULL
        ELSE str_to_map(tls, '\;', '=')
        END as tls_map,
    ch_ua,
    ch_ua_mobile,
    ch_ua_platform,
    ch_ua_arch,
    ch_ua_bitness,
    ch_ua_full_version_list,
    ch_ua_model,
    ch_ua_platform_version,
    get_referer_data(referer) as referer_data

FROM distinct_rows_and_reused_fields ;
