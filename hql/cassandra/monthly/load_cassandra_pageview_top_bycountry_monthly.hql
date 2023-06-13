-- Load pageview top countries monthly data to cassandra
-- Parameters:
--     destination_table     -- Cassandra table to write query output.
--     source_table          -- Fully qualified hive table to compute from.
--     year                   -- year of partition to compute from.
--     month                  -- month of partition to compute from.
--     coalesce_partitions    -- number of partitions for destination data.

-- Usage:
-- spark-sql \
-- --driver-cores 1 \
-- --master yarn \
-- --conf spark.sql.catalog.aqs=com.datastax.spark.connector.datasource.CassandraCatalog \
-- --conf spark.sql.catalog.aqs.spark.cassandra.connection.host=aqs1010-a.eqiad.wmnet:9042,aqs1011-a.eqiad.wmnet:9042,aqs1012-a.eqiad.wmnet:9042 \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.username=aqsloader \
-- --conf spark.sql.catalog.aqs.spark.cassandra.auth.password=cassandra \
-- --conf spark.sql.catalog.aqs.spark.cassandra.output.batch.size.rows=1024 \
-- --jars /srv/deployment/analytics/refinery/artifacts/org/wikimedia/analytics/refinery/refinery-job-0.2.17-shaded.jar \
-- --conf spark.dynamicAllocation.maxExecutors=128 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=3072  \
-- --executor-memory 8G \
-- --executor-cores 2 \
-- --driver-memory 4G \
--     -f load_cassandra_pageview_top_bycountry_monthly.hql \
--     -d destination_table=aqs.local_group_default_T_top_bycountry.data \
--     -d source_table=wmf.projectview_hourly \
--     -d country_deny_list_table=canonical_data.countries \
--     -d coalesce_partitions=6 \
--     -d year=2022 \
--     -d month=07

WITH ranked AS (
    SELECT
        project,
        access,
        country,
        year,
        month,
        views,
        rank() OVER (PARTITION BY project, access, year, month ORDER BY raw_views DESC) as rank,
        row_number() OVER (PARTITION BY project, access, year, month ORDER BY raw_views DESC) as rn
    FROM (
        SELECT
            COALESCE(project, 'all-projects') AS project,
            COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') AS access,
            country_code as country,
            LPAD(year, 4, '0') as year,
            LPAD(month, 2, '0') as month,
            SUM(view_count) as raw_views,
            CEIL(SUM(view_count) / 1000) * 1000 as views
        FROM ${source_table}
        WHERE
            year = ${year}
            AND month = ${month}
            AND agent_type = 'user'
        GROUP BY project, access_method, country_code, year, month
        GROUPING SETS (
            (project, access_method, country_code, year, month),
            (project, country_code, year, month),
            (access_method, country_code, year, month),
            (country_code, year, month)
        )
        HAVING SUM(view_count) > 99
    ) raw
)
INSERT INTO ${destination_table}
SELECT /*+ COALESCE(${coalesce_partitions}), BROADCAST(denied_countries) */
    'analytics.wikimedia.org' as _domain,
    ranked.project as project,
    ranked.access as access,
    ranked.year as year,
    ranked.month as month,
    '13814000-1dd2-11b2-8080-808080808080' as _tid,
    CONCAT('[',
        CONCAT_WS(',', collect_list(
            CONCAT('{"country":"', ranked.country,
                '","views":', CAST(ranked.views AS STRING),
                ',"rank":', CAST(ranked.rank AS STRING), '}'))
    ),']') as countriesJSON
FROM ranked
LEFT ANTI JOIN ${country_deny_list_table} denied_countries
    ON ranked.country = denied_countries.iso_code
        AND denied_countries.is_protected IS TRUE
GROUP BY
    ranked.project,
    ranked.access,
    ranked.year,
    ranked.month;
