-- Extracts one month of formatted monthly per-domain unique devices to be loaded in Druid
--
-- Usage:
--     spark-sql -f generate_druid_unique_devices_per_domain_monthly.hql \
--         -d source_table=wmf.unique_devices_per_domain_monthly \
--         -d destination_table=tmp_druid_unique_devices_per_domain_monthly_2023_01 \
--         -d destination_directory=/wmf/tmp/druid/unique_devices_per_domain_monthly_json \
--         -d year=2023 \
--         -d month=1
--


DROP TABLE IF EXISTS ${destination_table};


CREATE TABLE IF NOT EXISTS ${destination_table} (
    `dt`                     string,
    `domain`                 string,
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
        domain AS filtered_domain,
        SUM(uniques_estimate) AS checked_uniques_estimate
    FROM ${source_table}
    WHERE year=${year}
      AND month=${month}
    GROUP BY
        domain
    HAVING
        SUM(uniques_estimate) >= 1000
)

INSERT OVERWRITE TABLE ${destination_table}
SELECT /*+ COALESCE(1) */
    CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-01T00:00:00Z') AS dt,
    domain AS domain,
    country AS country,
    country_code AS country_code,
    uniques_underestimate AS uniques_underestimate,
    uniques_offset AS uniques_offset,
    uniques_estimate AS uniques_estimate
FROM ${source_table}
    INNER JOIN filtered_domains
        ON domain = filtered_domain
WHERE year = ${year}
    AND month = ${month};
