-- Overview:
--      This query works like this:
--          * filters pageview_actor for user (not spider nor automated)
--            pageviews to wikis with referrers from a wiki in the same project family
--          * aggregates view count by project family, source project (previous)
--            and destination project (current)
--
-- Parameters:
--      source_table      -- Fully qualified table name to compute the
--                           aggregation from.
--      refinery_hive_jar_path
--                        -- The hdfs path to the refinery-hive jar to use for UDFS
--      coalesce_partitions
                          -- Number of partitions to write at the end of the query
--      destination_table -- Fully qualified table name to fill in
--                           aggregated values.
--      year              -- year of partition to aggregate
--      month             -- month of partition to aggregate
--      day               -- day of partition to aggregate
--
-- Usage:
--     spark3-sql -f interlanguage_navigation.hql                                           \
--         -d refinery_hive_jar_path=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/refinery-hive.jar    \
--         -d coalesce_partitions=1                                                         \
--         -d source_table=wmf.pageview_actor                                               \
--         -d destination_table=wmf_traffic.interlanguage_navigation                        \
--         -d year=2017                                                                     \
--         -d month=10                                                                      \
--         -d day=03
--
-- example: ADD JAR hdfs://analytics-hadoop/wmf/refinery/current/artifacts/refinery-hive.jar;
ADD JAR ${refinery_hive_jar_path};

CREATE TEMPORARY FUNCTION normalize_host AS 'org.wikimedia.analytics.refinery.hive.GetHostPropertiesUDF';

-- Delete existing data for the period to prevent duplication of data in case of recomputation
DELETE FROM ${destination_table}
WHERE day = TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0')), 'yyyy-MM-dd');

INSERT OVERWRITE TABLE ${destination_table}
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        normalized_host.project_family,
        normalized_host.project AS current_project,
        normalize_host(parse_url(referer, 'HOST')).project as previous_project,
        COUNT(*) AS navigation_count,
        TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0')), 'yyyy-MM-dd') AS day

    FROM ${source_table}

    WHERE year=${year} AND month=${month} AND day=${day}
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
    ORDER BY project_family, current_project, previous_project
;
