-- Create table statement for the anomaly detection table.
--
-- Usage:
--     sudo -u analytics hive -f create_anomaly_detection_table.hql
--

CREATE EXTERNAL TABLE IF NOT EXISTS `wmf.anomaly_detection` (
    `dt`         string   COMMENT 'Date of the metric value (YYYY-MM-DD).',
    `metric`     string   COMMENT 'Name of the metric.',
    `value`      double   COMMENT 'Value of the metric.'
)
PARTITIONED BY (
    `source`     string   COMMENT 'Name of the job that produced the metric.',
    `year`       int      COMMENT 'Year of the metric value.',
    `month`      int      COMMENT 'Month of the metric value.',
    `day`        int      COMMENT 'Day of the metric value.'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/anomaly_detection'
;
