--
-- Creates a table to store hourly data quality metrics.
-- See: oozie/data_quality/README.md
--
-- Usage
--     hive -f create_data_quality_hourly_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `data_quality_hourly` (
    `metric`         string   COMMENT 'Name of the metric.',
    `value`          double   COMMENT 'Value of the metric.'
)
PARTITIONED BY (
    `source_table`   string   COMMENT 'Fully qualified name of the Hive table the metric was extracted from.',
    `query_name`     string   COMMENT 'Name of the query used to extract metrics (without .hql).',
    `year`           int      COMMENT 'Unpadded year.',
    `month`          int      COMMENT 'Unpadded month.',
    `day`            int      COMMENT 'Unpadded day.',
    `hour`           int      COMMENT 'Unpadded hour.'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/wmf/data/wmf/data_quality/hourly'
;
