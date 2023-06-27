-- Load pageview top-per-country daily to Cassandra

-- Parameters:
--     destination_table                   -- Cassandra table to write query output.
--     source_table                        -- Fully qualified hive table to compute from.
--     country_deny_list_table             -- Fully qualified table name containing the countries that should be
--                                            excluded from the results
--     disallowed_cassandra_articles_table -- Fully qualified hive table containing article titles we don't want to
--                                            appear in the top list (ex: offensive language, DOS attack
--                                            manipulations,...).
--     year                                -- year of partition to compute from.
--     month                               -- month of partition to compute from.
--     day                                 -- day of partition to compute from.
--     coalesce_partitions                 -- number of partitions for destination data.
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
-- --jars /srv/deployment/analytics/refinery/artifacts/org/wikimedia/analytics/refinery/refinery-job-0.2.17-shaded.jar  \
-- --conf spark.dynamicAllocation.maxExecutors=64 \
-- --conf spark.yarn.maxAppAttempts=1 \
-- --conf spark.executor.memoryOverhead=2048  \
-- --executor-memory 8G \
-- --executor-cores 2 \
-- --driver-memory 4G \
-- -f load_cassandra_pageview_top_percountry_daily.hql \
-- -d destination_table=aqs.local_group_default_T_top_percountry.data \
-- -d source_table=wmf.pageview_actor \
-- -d country_deny_list_table=canonical_data.countries \
-- -d disallowed_cassandra_articles_table=wmf.disallowed_cassandra_articles \
-- -d coalesce_partitions=6 \
-- -d year=2022 \
-- -d month=07 \
-- -d day=01


WITH base_data AS (
    SELECT /*+ BROADCAST(disallowed_articles_list), BROADCAST(country_deny_list) */
        geocoded_data['country_code'] AS country_code,
        REGEXP_REPLACE(access_method, ' ', '-') AS access,
        pageview_info['project'] AS project,
        REFLECT('org.json.simple.JSONObject', 'escape', REGEXP_REPLACE(pageview_info['page_title'], '\t', '')) AS page_title,
        LPAD(year, 4, '0') as year,
        LPAD(month, 2, '0') as month,
        LPAD(day, 2, '0') as day,
        actor_signature
    FROM ${source_table} source
    LEFT ANTI JOIN ${disallowed_cassandra_articles_table} disallowed_articles_list
        ON pageview_info['project'] = disallowed_articles_list.project
            AND lower(pageview_info['page_title']) = lower(disallowed_articles_list.article)
    LEFT ANTI JOIN ${country_deny_list_table} country_deny_list
        ON country_deny_list.iso_code = source.geocoded_data['country_code']
            AND country_deny_list.is_protected IS TRUE
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
        AND agent_type = 'user'
        AND pageview_info IS NOT NULL
        AND geocoded_data IS NOT NULL
        AND is_pageview
        AND pageview_info['page_title'] != '-'
        AND geocoded_data['country_code'] != '--'
),
raw AS (
    SELECT
        country_code,
        COALESCE(access, 'all-access') AS access,
        project,
        page_title,
        year,
        month,
        day,
        COUNT(1) AS total_view_count,
        COUNT(DISTINCT actor_signature) AS unique_actor_count
    FROM base_data
    GROUP BY
        country_code,
        access,
        project,
        page_title,
        year,
        month,
        day
    GROUPING SETS (
        (
            country_code,
            access,
            project,
            page_title,
            year,
            month,
            day
        ),
        (
            country_code,
            project,
            page_title,
            year,
            month,
            day
        )
    )
),
ranked AS (
    SELECT
        country_code,
        access,
        project,
        page_title,
        year,
        month,
        day,
        CEIL(total_view_count / 100) * 100 AS views_ceil,
        rank() OVER (PARTITION BY access, country_code, year, month, day ORDER BY total_view_count DESC) as rank,
        row_number() OVER (PARTITION BY access, country_code, year, month, day ORDER BY total_view_count DESC) as rn
    FROM raw
    WHERE unique_actor_count > 1000
),
max_rank AS (
    SELECT
        country_code as max_rank_country_code,
        access as max_rank_access,
        year as max_rank_year,
        month as max_rank_month,
        day as max_rank_day,
        rank as max_rank
    FROM ranked
    WHERE rn = 1001
    GROUP BY
        country_code,
        access,
        year,
        month,
        day,
        rank
)
INSERT INTO ${destination_table}
SELECT
/*+ COALESCE(${coalesce_partitions}) */
  'analytics.wikimedia.org' as _domain,
  country_code as country,
  access as access,
  year as year,
  month as month,
  day as day,
  '13814000-1dd2-11b2-8080-808080808080' as _tid,
  CONCAT(
      '[',
      CONCAT_WS(
          ',',
          COLLECT_LIST(
              CONCAT(
                  '{"article":"',
                  page_title,
                  '","project":"',
                  project,
                  '","views_ceil":',
                  CAST(views_ceil AS STRING),
                  ',"rank":',
                  CAST(rank AS STRING),
                  '}'
              )
          )
      ),
      ']'
  ) as articles
FROM ranked
LEFT JOIN max_rank ON (
    country_code = max_rank_country_code
    AND access = max_rank_access
    AND year = max_rank_year
    AND month = max_rank_month
    AND day = max_rank_day
)
WHERE
    rank < COALESCE(max_rank, 1001)
GROUP BY
    country_code,
    access,
    year,
    month,
    day;