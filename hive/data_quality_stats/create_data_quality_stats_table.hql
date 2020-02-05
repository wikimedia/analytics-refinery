--
-- Creates a table to store data quality stats.
-- See: oozie/data_quality_stats/README.md
--
-- Two tables need to be created with this script:
-- One to store the historical data in a compacted way (no time partitions),
-- and one to temporary store the computations of incoming data points.
--
-- To create the incoming table:
--     sudo -u analytics hive -f create_data_quality_stats_table.hql \
--         --database analytics \
--         -d table_name='data_quality_stats_incoming' \
--         -d location='/user/analytics/data/data_quality_stats_incoming'
--
-- To create the historical table:
--     sudo -u analytics hive -f create_data_quality_stats_table.hql \
--         --database wmf \
--         -d table_name='data_quality_stats' \
--         -d location='/wmf/data/wmf/data_quality_stats'
--

CREATE EXTERNAL TABLE IF NOT EXISTS `${table_name}` (
    `dt`             string   COMMENT 'UTC ISO 8601 timestamp cropped to query granularity (2019-08-27T14:00:00Z).',
    `metric`         string   COMMENT 'Name of the metric.',
    `value`          double   COMMENT 'Value of the metric.'
)
PARTITIONED BY (
    `source_table`   string   COMMENT 'Fully qualified name of the Hive table the metric was extracted from.',
    `query_name`     string   COMMENT 'Name of the query used to extract metrics (without .hql).',
    `granularity`    string   COMMENT 'The granularity of the metric: hourly, daily or monthly.'
)
STORED AS PARQUET
LOCATION '${location}'
;
