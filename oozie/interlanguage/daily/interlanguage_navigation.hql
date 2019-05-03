-- Overview:
--      This query works like this:
--          * filters webrequest for user (not spider) pageviews to wikis
--            with referrers from a wiki in the same project family
--          * aggregates view count by project family, source project (previous)
--            and destination project (current)
--
-- Parameters:
--      source_table      -- Fully qualified table name to compute the
--                           aggregation from.
--      artifacts_directory
--                        -- The artifact directory where to find
--                           jar files to import for UDFs
--      refinery_jar_version
--                        -- Version of the jar to import for UDFs
--      destination_table -- Fully qualified table name to fill in
--                           aggregated values.
--      year              -- year of partition to aggregate
--      month             -- month of partition to aggregate, left zero-padded
--      day               -- day of partition to aggregate, left zero-padded
--
-- Usage:
--     hive -f interlanguage_links.hql                                                      \
--         -d artifacts_directory=hdfs://analytics-hadoop/wmf/refinery/current/artifacts    \
--         -d refinery_jar_version=0.0.53                                                   \
--         -d source_table=wmf.webrequest                                                   \
--         -d destination_table=wmf.interlanguage_links                                     \
--         -d year=2017                                                                     \
--         -d month=10                                                                      \
--         -d day=03
--
-- example: ADD JAR hdfs://analytics-hadoop/wmf/refinery/current/artifacts/org/wikimedia/analytics/refinery/refinery-hive-0.0.53.jar;
ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;

SET parquet.compression = SNAPPY;
CREATE TEMPORARY FUNCTION normalize_host AS 'org.wikimedia.analytics.refinery.hive.GetHostPropertiesUDF';

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(date='${year}-${month}-${day}')

     SELECT normalized_host.project_family,
            normalized_host.project AS current_project,
            normalize_host(parse_url(referer, 'HOST')).project as previous_project,
            COUNT(*) AS navigation_count

       FROM ${source_table}

      WHERE webrequest_source='text'
        AND year=${year} AND month=${month} AND day=${day}
        AND is_pageview
        AND agent_type = 'user'
        -- The project is the same as the referer project
        AND normalized_host.project_family = normalize_host(parse_url(referer, 'HOST')).project_family
        -- The referer host has no .m, or other qualifiers
        AND size(normalize_host(parse_url(referer, 'HOST')).qualifiers) = 0
        -- The referer path was something with a /wiki/ beginning, like a normal article path
        AND parse_url(referer,'PATH') LIKE '/wiki/%'
        AND normalized_host.project <> normalize_host(parse_url(referer, 'HOST')).project

      GROUP BY normalized_host.project_family,
            normalized_host.project,
            normalize_host(parse_url(referer, 'HOST')).project
;
