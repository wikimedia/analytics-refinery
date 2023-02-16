-- Parameters:
--     refinery_jar_version
--                       -- Version of the jar to import for UDFs
--     artifacts_directory
--                       -- The artifact directory where to find
--                          jar files to import for UDFs
--     source_table      -- Fully qualified table name to compute the
--                          statistics for.
--     destination_table -- Fully qualified table name to stopre the
--                          computed statistics in. This table should
--                          have schema described in [1].
--     webrequest_source -- webrequest_source of partition to compute
--                          statistics for.
--     record_version    -- record_version keeping track of changes
--                          in the table content definition.
--     year              -- year of partition to compute statistics
--                          for.
--     month             -- month of partition to compute statistics
--                          for.
--     day               -- day of partition to compute statistics
--                          for.
--     hour              -- hour of partition to compute statistics
--                          for.
--
-- Usage:
--     hive -f refine_webrequest.hql                              \
--         -d refinery_jar_version=X.X.X                          \
--         -d artifacts_directory=/wmf/refinery/current/artifacts \
--         -d source_table=wmf_raw.webrequest                     \
--         -d destination_table=wmf.webrequest                    \
--         -d webrequest_source=text                              \
--         -d record_version=0.0.1                                \
--         -d year=2014                                           \
--         -d month=12                                            \
--         -d day=30                                              \
--         -d hour=1
--

SET parquet.compression              = SNAPPY;
SET hive.enforce.bucketing           = true;
-- mapreduce.job.reduces should not be necessary to
-- specify since we set hive.enforce.bucketing=true.
-- However, without this set, only one reduce task is
-- launched, so we set it manually.  This needs
-- to be the same as the number of buckets the
-- table is clustered by.
SET mapreduce.job.reduces            = 256;

-- Memory settings for mappers to provide user-agent UDF with enough space
-- for caching (see https://phabricator.wikimedia.org/T240815)
SET mapreduce.map.memory.mb=3072;
SET mapreduce.map.java.opts=-Xmx2458m;

ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;
ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}-shaded.jar;
CREATE TEMPORARY FUNCTION is_pageview as 'org.wikimedia.analytics.refinery.hive.IsPageviewUDF';
CREATE TEMPORARY FUNCTION geocoded_data as 'org.wikimedia.analytics.refinery.hive.GeocodedDataUDF';
CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
CREATE TEMPORARY FUNCTION get_access_method as 'org.wikimedia.analytics.refinery.hive.GetAccessMethodUDF';
CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
CREATE TEMPORARY FUNCTION referer_classify AS 'org.wikimedia.analytics.refinery.hive.SmartReferrerClassifierUDF';
CREATE TEMPORARY FUNCTION get_pageview_info AS 'org.wikimedia.analytics.refinery.hive.GetPageviewInfoUDF';
CREATE TEMPORARY FUNCTION normalize_host AS 'org.wikimedia.analytics.refinery.hive.HostNormalizerUDF';
CREATE TEMPORARY FUNCTION get_tags AS 'org.wikimedia.analytics.refinery.hive.GetWebrequestTagsUDF';
CREATE TEMPORARY FUNCTION isp_data as 'org.wikimedia.analytics.refinery.hive.GetISPDataUDF';
CREATE TEMPORARY FUNCTION get_referer_data as 'org.wikimedia.analytics.refinery.hive.GetRefererDataUDF';


-- The distinct_rows CTE provides DISTINCT on raw data only.
-- This prevents augmented fields to be shuffled,
-- therefore reduces IO cost significantly.
-- NB: This is feasible as augmented values are
--     deterministically computed.
--
-- The distinct_rows_and_reused_fields CTE materializes
-- reused fields in the reduce step, preventing recomputation
-- at every reuse.
--
-- Finally compute fields not reused and write the data.
--
-- When adding new fields:
--  * fields imported from the wmf_raw.webrequest table
--    need to be included in the two CTEs and the main SELECT
--  * fields computed from fields already present in the
--    distinct_rows CTE and reused multiple times in the main
--    select need to be added to the distinct_rows_and_reused_fields
--    CTE, to be reused in the main SELECT
--  * fields computed from fields already present in any
--    CTE and used a single time in the main select need to be
--    added to the main SELECT only


WITH distinct_rows AS (

    SELECT DISTINCT
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
    WHERE
        webrequest_source='${webrequest_source}' AND
        year=${year} AND month=${month} AND day=${day} AND hour=${hour}

),

distinct_rows_and_reused_fields AS (

    SELECT DISTINCT
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
        -- Materialize reused computed fields
        is_pageview(uri_host, uri_path, uri_query, http_status, content_type, user_agent, x_analytics) as is_pageview,
        ua_parser(user_agent) as user_agent_map,
        CASE COALESCE(x_analytics, '-')
          WHEN '-' THEN NULL
          ELSE str_to_map(x_analytics, '\;', '=')
        END as x_analytics_map,
        ch_ua,
        ch_ua_mobile,
        ch_ua_platform,
        ch_ua_arch,
        ch_ua_bitness,
        ch_ua_full_version_list,
        ch_ua_model,
        ch_ua_platform_version
    FROM distinct_rows

)

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(webrequest_source='${webrequest_source}',year=${year},month=${month},day=${day},hour=${hour})
    -- No need for DISTINCT here as it enforced in distinct_rows CTE
    SELECT
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
        ip as client_ip,
        geocoded_data(ip) as geocoded_data,
        x_cache,
        user_agent_map,
        x_analytics_map,
        -- Hack to get a correct timestamp because of hive inconsistent conversion
        CAST(unix_timestamp(dt, "yyyy-MM-dd'T'HH:mm:ss") * 1.0 as timestamp) as ts,
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
        x_analytics_map['page_id'] as page_id,
        x_analytics_map['ns'] as namespace_id,
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
    FROM distinct_rows_and_reused_fields
;
