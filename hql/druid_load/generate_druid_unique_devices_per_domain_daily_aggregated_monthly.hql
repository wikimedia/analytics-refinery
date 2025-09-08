-- Extracts one month of formatted daily per-domain unique devices to be loaded in Druid
--
-- Usage:
--     spark3-sql -f generate_druid_unique_devices_per_domain_daily_aggregated_monthly.hql \
--         -d source_table=wmf_readership.unique_devices_per_domain_daily \
--         -d destination_table=tmp_druid_unique_devices_per_domain_daily_aggregated_monthly_2023_01 \
--         -d destination_directory=/wmf/tmp/druid/unique_devices_per_domain_daily_json \
--         -d day=2023-01-01
--


DROP TABLE IF EXISTS ${destination_table};


CREATE TABLE IF NOT EXISTS ${destination_table} (
    `dt`                     string,
    `domain`                 string,
    `access_method`          string,
    `country`                string,
    `country_code`           string,
    `uniques_underestimate`  bigint,
    `uniques_offset`         bigint,
    `uniques_estimate`       bigint
)
USING PARQUET
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';


WITH filtered_domains AS (
    SELECT
        domain,
        day,
        SUM(uniques_estimate) AS checked_uniques_estimate
    FROM ${source_table}
    WHERE day >= TO_DATE('${day}', 'yyyy-MM-dd')
        AND day < ADD_MONTHS(TO_DATE('${day}', 'yyyy-MM-dd'), 1)
    GROUP BY
        domain,
        day
    HAVING
        SUM(uniques_estimate) >= 1000
)

INSERT OVERWRITE TABLE ${destination_table}
SELECT /*+ COALESCE(1) */
    date_format(day, 'yyyy-MM-dd\'T\'HH:mm:ss.SSS\'Z\'') AS dt,
    domain AS domain,
    access_method AS access_method,
    country AS country,
    country_code AS country_code,
    uniques_underestimate AS uniques_underestimate,
    uniques_offset AS uniques_offset,
    uniques_estimate AS uniques_estimate
FROM ${source_table} s
    INNER JOIN filtered_domains using (domain, day)
WHERE day >= TO_DATE('${day}', 'yyyy-MM-dd')
    AND day < ADD_MONTHS(TO_DATE('${day}', 'yyyy-MM-dd'), 1);
