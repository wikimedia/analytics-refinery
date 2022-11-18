-- Aggregates data from unique_devices_per_domain_monthly
--
-- Parameters:
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--     metric               -- Metric to compute statistics for
--
-- Usage:
--     hive -f georeadership_input_metrics_unique_devices.hql               \
--          -d source_table=wmf.unique_devices_per_domain_monthly           \
--          -d destination_table=gdi.georeadership_input_metrics            \
--          -d year=2021
--          -d metric=unique_devices
--

INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}', metric='${metric}')
SELECT country_code,
       sum(uniques_estimate) as sum_unique_estimate
  FROM ${source_table}
 WHERE year = ${year}
 GROUP BY country_code;