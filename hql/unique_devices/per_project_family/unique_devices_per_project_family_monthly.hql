-- Generates unique devices per-project-family monthly based on WMF-Last-Access-global cookie
--
-- Parameters:
--     source_table         -- Table containing source data
--     destination_table    -- Table where to write newly computed data
--     countries_table      -- Canonical names of countries
--     year                 -- year of the to-be-generated
--     month                -- month of the to-be-generated
--     coalesce_partitions  -- Number of partitions to write
--
-- Usage (use best with spark-submit to be able to use deploy-mode=cluster):
--     spark3-submit \
--         --master yarn \
--         --deploy-mode cluster \
--         --driver-cores 1 \
--         --driver-memory 4G \
--         --executor-cores 4 \
--         --executor-memory 16G \
--         --conf spark.dynamicAllocation.maxExecutors=32 \
--         --conf spark.yarn.executor.memoryOverhead=2048 \
--         --conf spark.sql.shuffle.partitions=512 \
--         --conf spark.yarn.maxAppAttempts=1 \
--         --conf spark.yarn.archive=hdfs://analytics-hadoop/user/spark/share/lib/spark-3.1.2-assembly.jar \
--         --class org.apache.spark.sql.hive.thriftserver.SparkSQLNoCLIDriver \
--         hdfs://analytics-hadoop/wmf/refinery/current/artifacts/refinery-job-shaded.jar \
--         -f hdfs://analytics-hadoop/user/milimetric/unique_devices_per_project_family_monthly.hql \
--         -d source_table=wmf.pageview_actor \
--         -d destination_table=milimetric.unique_devices_per_project_family_monthly_candidate \
--         -d countries_table=canonical_data.countries \
--         -d year=2023 \
--         -d month=11 \
--         -d coalesce_partitions=1


WITH last_access_dates AS (
    SELECT
        year,
        month,
        normalized_host.project_class AS project_family,
        geocoded_data['country_code'] AS country_code,
        -- Sometimes (~1 out of 1B times) WMF-Last-Access-Global is corrupted.
        -- and Spark can not parse it. Check for the length of the string.
        IF(length(x_analytics_map['WMF-Last-Access-Global']) = 11,
           unix_timestamp(x_analytics_map['WMF-Last-Access-Global'], 'dd-MMM-yyyy'),
           NULL) AS last_access_global,
        x_analytics_map['nocookies'] AS nocookies,
        actor_signature_per_project_family AS actor_signature
    FROM ${source_table}
    WHERE x_analytics_map IS NOT NULL
      AND agent_type = 'user'
      AND (is_pageview OR is_redirect_to_pageview)
      AND year = ${year}
      AND month = ${month}
),

-- Only keeping clients having 1 event without cookies
-- (fresh sessions are not counted with last_access method)
fresh_sessions_aggregated AS (
    SELECT
        project_family,
        country_code,
        COUNT(1) AS uniques_offset
    FROM (
        SELECT
            actor_signature,
            project_family,
            country_code,
            SUM(CASE WHEN (nocookies IS NOT NULL) THEN 1 ELSE 0 END)
        FROM
            last_access_dates
        GROUP BY
            actor_signature,
            project_family,
            country_code
        -- Only keeping clients having done 1 event without cookies
        HAVING SUM(CASE WHEN (nocookies IS NOT NULL) THEN 1 ELSE 0 END) = 1
        ) fresh_sessions
    GROUP BY
        project_family,
        country_code
),

-- Aggregate last access uniques before joining with fresh sessions.
-- Otherwise, Spark has trouble joining the data because it's skewed.
-- Also, calculate uniques_underestimate.
last_access_uniques_aggregated AS (
    SELECT
        project_family,
        country_code,
        SUM(CASE
            -- project_family set, last-access-global not set and client accept cookies --> first visit, count
            WHEN (project_family IS NOT NULL AND last_access_global IS NULL AND nocookies is NULL) THEN 1
            -- last-access-global set and its date is before today --> First visit today, count
            WHEN ((last_access_global IS NOT NULL)
                AND (last_access_global < unix_timestamp(CONCAT('${year}-', LPAD('${month}', 2, '0'), '-01'), 'yyyy-MM-dd'))) THEN 1
            -- Other cases, don't count
            ELSE 0
        END) AS uniques_underestimate
    FROM
        last_access_dates
    GROUP BY
        project_family,
        country_code
)

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year = ${year}, month = ${month})

-- Join last_access_uniques_aggregated with fresh_sessions_aggregated
-- to calculate uniques_estimate = uniques_underestimate + uniques_offset.
SELECT /*+ COALESCE(${coalesce_partitions}) */
    last_access_uniques.project_family,
    COALESCE(countries.name, '(missing country name)'),
    last_access_uniques.country_code,
    last_access_uniques.uniques_underestimate,
    COALESCE(fresh_sessions.uniques_offset, 0) AS uniques_offset,
    last_access_uniques.uniques_underestimate + COALESCE(fresh_sessions.uniques_offset, 0) AS uniques_estimate
FROM
    last_access_uniques_aggregated AS last_access_uniques
    LEFT JOIN ${countries_table} AS countries
        ON countries.iso_code = last_access_uniques.country_code
    -- Outer join to keep every row from both table
    FULL OUTER JOIN fresh_sessions_aggregated AS fresh_sessions
        -- No need to add country here as country_code matches
        ON last_access_uniques.project_family = fresh_sessions.project_family
          AND last_access_uniques.country_code = fresh_sessions.country_code
;
