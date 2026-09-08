--
-- Computes projectview_hourly legacy data from pageview_actor (instead of from pageview_hourly).
--
-- In September 2026 (https://phabricator.wikimedia.org/T425661) we replaced the legacy
-- bot detection system with KAPOW. As part of this change, the pageview_hourly pipeline
-- stopped consuming pageview_actor as source data, and started consuming webrequest_v2
-- data instead. This means that all datasets downstream of it, like projectview_hourly,
-- started showing KAPOW-sourced data.
--
-- Since KAPOW's algorithm was a complete revamp of the legacy bot detection system,
-- we wanted to keep both the legacy version and the KAPOW version of the pageview data,
-- alive in parallel for a couple months, to be able to honor yearly legacy metrics, and compare.
-- However, keeping 2 versions of pageview_hourly seemed too expensive re. cluster storage.
-- And we decided to keep a legacy version of projectview_hourly, which is lighter.
--
-- This script generates projectview_legacy_hourly data by directly querying pageview_actor,
-- the downstream-most dataset in the legacy bot detection system. The legacy bot detection
-- system should also be kept alive for as long as we need projectview_legacy_hourly.
--
-- We use the same create_pageview_actor_table.hql file to create this legacy version,
-- since they share the exact same schema.
--
-- Parameters:
--     pageview_actor_table  -- Fully qualified pageview_actor table name
--                              to compute the aggregation from.
--     destination_table     -- Fully qualified output table name.
--     record_version        -- Record_version keeping track of changes
--                              in the table content definition.
--     year                  -- Year of partition to compute statistics for.
--     month                 -- Month of partition to compute statistics for.
--     day                   -- Day of partition to compute statistics for.
--     hour                  -- Hour of partition to compute statistics for.
--     coalesce_partitions   -- The number of files to write.
--
-- Usage:
--     spark3-sql \
--     --master yarn \
--     --deploy-mode client \
--     --driver-cores 1 \
--     --driver-memory 4G \
--     --executor-cores 2 \
--     --executor-memory 8G \
--     --conf spark.dynamicAllocation.maxExecutors=32 \
--     --conf spark.executor.memoryOverhead=2G \
--     --conf spark.yarn.maxAppAttempts=1 \
--     -f hdfs://analytics-hadoop/user/mforns/queries/aggregate_pageview_actor_to_projectview.hql \
--     -d pageview_actor_table=wmf.pageview_actor \
--     -d destination_table=wmf.projectview_legacy_hourly \
--     -d record_version=0.0.1 \
--     -d year=2026 \
--     -d month=9 \
--     -d day=1 \
--     -d hour=0 \
--     -d coalesce_partitions=1
--

SET parquet.compression = SNAPPY;

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(
        year = ${year},
        month = ${month},
        day = ${day},
        hour = ${hour}
    )
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        pageview_info['project'] AS project,
        access_method,
        NULL AS zero_carrier,
        agent_type,
        referer_data['referer_class'] AS referer_class,
        geocoded_data['continent'] AS continent,
        geocoded_data['country_code'] AS country_code,
        '${record_version}' AS record_version,
        count(*) AS view_count
    FROM
        ${pageview_actor_table}
    WHERE
        year = ${year} AND
        month = ${month} AND
        day = ${day} AND
        hour = ${hour} AND
        is_pageview AND
        coalesce(pageview_info['project'], '') != ''
    GROUP BY
        pageview_info['project'],
        access_method,
        agent_type,
        referer_data['referer_class'],
        geocoded_data['continent'],
        geocoded_data['country_code']
;
