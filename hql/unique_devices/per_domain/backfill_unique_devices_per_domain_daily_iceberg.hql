-- Backfills the daily per-domain unique devices from the original hive table.
--
-- The new Iceberg table being different with a new access_method column, the backfill
-- changes the original data by parsing the domains to extract the ones having a `.m`
-- subdomain.
--
-- NOTE: The destination table is expected to be empty. No deletion is made
--       prior to loading the data, and if the destionation table already contains
--       for the period it is backfilled, duplication will happen.
--
-- Parameters:
--     unique_devices_hive_table     -- Table containing source data
--     unique_devices_iceberg_table  -- Table where to write newly computed data
--
-- Usage:
--     spark3-sql \
--         --master yarn \
--         --deploy-mode client \
--         --driver-cores 1 \
--         --driver-memory 4G \
--         --executor-cores 2 \
--         --executor-memory 8G \
--         --conf spark.dynamicAllocation.maxExecutors=64 \
--         --conf spark.yarn.executor.memoryOverhead=2048 \
--         --conf spark.yarn.maxAppAttempts=1 \
--         -f backfill_unique_devices_per_domain_daily_iceberg.hql \
--         -d unique_devices_hive_table=wmf.unique_devices_per_domain_daily \
--         -d unique_devices_iceberg_table=wmf_readership.unique_devices_per_domain_daily

INSERT INTO ${unique_devices_iceberg_table}

SELECT
    CONCAT(
        regexp_extract(domain, '^((?!(www\\.|m\\.))([a-z0-9-_]+\\.))(m\\.)?\\w+\\.org\\.?$$'),
        regexp_extract(domain, '([a-z0-9-_]+)\\.org\\.?$$'),
        '.org'
    ) AS domain,
    CASE
        WHEN domain RLIKE '(^(m)\\.)|\\.m\\.' THEN 'mobile web'
        ELSE 'desktop'
    END AS access_method,
    country,
    country_code,
    SUM(uniques_underestimate),
    SUM(uniques_offset),
    SUM(uniques_estimate),
    TO_DATE(CONCAT_WS('-', LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0')), 'yyyy-MM-dd') AS day
FROM ${unique_devices_hive_table}
GROUP BY
    CONCAT(
        regexp_extract(domain, '^((?!(www\\.|m\\.))([a-z0-9-_]+\\.))(m\\.)?\\w+\\.org\\.?$$'),
        regexp_extract(domain, '([a-z0-9-_]+)\\.org\\.?$$'),
        '.org'
    ),
    CASE
        WHEN domain RLIKE '(^(m)\\.)|\\.m\\.' THEN 'mobile web'
        ELSE 'desktop'
    END,
    country,
    country_code,
    TO_DATE(CONCAT_WS('-', LPAD(year, 4, '0'), LPAD(month, 2, '0'), LPAD(day, 2, '0')), 'yyyy-MM-dd')
DISTRIBUTE BY year(day)
SORT BY day, domain, access_method, country_code
;
