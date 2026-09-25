-- Compute the daily Google Search Console sitedata aggregate and write it to the Iceberg table.
--
-- Parameters:
--   source_table        -- Raw GSC site-impression Hive table to read
--                          (wmf_raw.google_search_console_site_impression)
--   destination_table   -- Iceberg table to write the aggregate to
--                          (wmf_readership.google_sitedata_daily_aggregate)
--   day                 -- day to (re)compute (YYYY-MM-DD); matches the raw table's `date` partition
--   coalesce_partitions -- Number of partitions to write at query end
--
-- Usage:
--   spark3-sql \
--     -f google_sitedata_daily_aggregate_iceberg.hql \
--     -d source_table=wmf_raw.google_search_console_site_impression \
--     -d destination_table=wmf_readership.google_sitedata_daily_aggregate \
--     -d coalesce_partitions=1 \
--     -d day=2025-08-27

DELETE FROM ${destination_table}
    WHERE `date` = '${day}';

INSERT INTO ${destination_table}

SELECT /*+ COALESCE(${coalesce_partitions}) */
    data_date,
    site_url,
    country,
    search_type,
    device,
    SUM(impressions) AS impressions,
    SUM(clicks)      AS clicks,
    `date`
FROM ${source_table}
WHERE `date` = '${day}'
GROUP BY
    data_date,
    site_url,
    country,
    search_type,
    device,
    `date`
;
