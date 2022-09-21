-- Loads the daily unique devices to cassandra
-- Parameters:
--     destination_table                 -- Cassandra table to write query output.
--     source_table_per_domain           -- Fully qualified hive table to compute from (per domain).
--     source_table_per_project_family   -- Fully qualified hive table to compute from (per family).
--     year                              -- year of partition to compute from.
--     month                             -- month of partition to compute from.
--     day                               -- day of partition to compute from.
--     coalesce_partitions               -- number of partitions for destination data.
--
-- Usage:
-- spark-sql \
-- --driver-cores 1 \
-- --master yarn \
-- --conf spark.sql.catalog.aqs=com.datastax.spark.connector.datasource.CassandraCatalog \
-- --conf spark.sql.catalog.aqs.spark.cassandra.connection.host=aqs1010-a.eqiad.wmnet:9042,aqs1011-a.eqiad.wmnet:9042,aqs1012-a.eqiad.wmnet:9042 \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.username=aqsloader \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.password=cassandra \
-- --conf spark.sql.catalog.aqs.spark.cassandra.output.batch.size.rows=1024 \
-- --jars /srv/deployment/analytics/refinery/artifacts/org/wikimedia/analytics/refinery/refinery-job-0.2.4-shaded.jar  \
-- --conf spark.dynamicAllocation.maxExecutors=64 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=2048  \
-- --executor-memory 8G \
-- --executor-cores 2 \
-- --driver-memory 4G \
-- --name spark_3_unique_devices \
--     -f load_cassandra_unique_devices_daily.hql \
--     -d destination_table=aqs.local_group_default_T_unique_devices.data \
--     -d source_table_per_domain=wmf.unique_devices_per_domain_daily \
--     -d source_table_per_project_family=wmf.unique_devices_per_project_family_daily \
--     -d coalesce_partitions=6 \
--     -d year=2022 \
--     -d month=07 \
--     -d day=01

WITH unique_devices_per_domain AS (
    SELECT
        CONCAT(
            regexp_extract(domain, '^((?!www)([a-z0-9-_]+)\\.)(m\\.)?\\w+\\.org$$'),
            regexp_extract(domain, '([a-z0-9-_]+)\\.org$$')
            ) AS project,
        CASE WHEN domain RLIKE '(^(m)\\.)|\\.m\\.'
            THEN 'mobile-site'
            ELSE 'desktop-site'
            END AS access_site,
        CONCAT(LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0')) AS dt,
        SUM(uniques_estimate) AS devices,
        SUM(uniques_offset) AS offset,
        SUM(uniques_underestimate) AS underestimate
    FROM
        ${source_table_per_domain}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY
        CONCAT(
            regexp_extract(domain, '^((?!www)([a-z0-9-_]+)\\.)(m\\.)?\\w+\\.org$$'),
            regexp_extract(domain, '([a-z0-9-_]+)\\.org$$')
            ),
        CASE WHEN domain RLIKE '(^(m)\\.)|\\.m\\.'
            THEN 'mobile-site'
            ELSE 'desktop-site'
            END,
        CONCAT(LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0'))
    HAVING SUM(uniques_estimate) > 1000
), unique_devices_per_domain_all_sites AS (
    SELECT
        project,
        'all-sites' AS access_site,
        dt,
        SUM(devices) AS devices,
        SUM(offset) AS offset,
        SUM(underestimate) AS underestimate
    FROM
        unique_devices_per_domain
    GROUP BY
        project,
        dt
), unique_devices_per_project_family AS (
    SELECT
        CONCAT('all-', project_family, '-projects') AS project,
        'all-sites' AS access_site,
        CONCAT(LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0')) AS dt,
        SUM(uniques_estimate) AS devices,
        SUM(uniques_offset) AS offset,
        SUM(uniques_underestimate) AS underestimate
    FROM
        ${source_table_per_project_family}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
        AND !array_contains(array('mediawiki', 'wikidata', 'wikimediafoundation', 'wikimedia'), project_family)
    GROUP BY
        CONCAT('all-', project_family, '-projects'),
        CONCAT(LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0'))
    HAVING SUM(uniques_estimate) > 1000
), unique_devices AS (
    SELECT * FROM unique_devices_per_domain
    UNION ALL
    SELECT * FROM unique_devices_per_domain_all_sites
    UNION ALL
    SELECT * FROM unique_devices_per_project_family
)
INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
    'analytics.wikimedia.org' as _domain,
    project,
    access_site as `access-site`,
    'daily' as granularity,
    dt as timestamp,
    '13814000-1dd2-11b2-8080-808080808080' as _tid,
    devices,
    offset,
    underestimate
FROM unique_devices