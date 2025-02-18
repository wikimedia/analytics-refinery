-- Create table statement for the data quality metrics table.
--
-- Usage:
--     sudo -u analytics spark3-sql -f create_data_quality_metrics_table.hql \
--     --database wmf_data_ops \
--     -d location=/wmf/data/data_quality/metrics/
--
CREATE EXTERNAL TABLE `data_quality_metrics` (
    dataset_date        BIGINT                  COMMENT 'AWS Deequ metric repo: key insertion time.',
    tags                MAP<STRING,STRING>      COMMENT 'AWS Deequ metric repo: key tags.',
    entity              STRING                  COMMENT 'AWS Deequ metric repo: the type of entity the metric is recorded against.e.g. A column, dataset, or multicolumn.',
    instance            STRING                  COMMENT 'AWS Deequ repo: information about this instance of the metric. For example this could be the column name the metric is computed on.',
    value               DOUBLE                  COMMENT 'AWS Deequ repo: the value of the metric at a point in time.',
    name                STRING                  COMMENT 'AWS Deequ repo: the name for the type of metric.',
    source_table        STRING                  COMMENT 'The table metrics are computed on.',
    pipeline_run_id     STRING                  COMMENT 'A unique identifier of the orchestrator that generated the metric.e.g. this could an Airflow run_id.',
    partition_id        STRING                  COMMENT 'Identifier of the partition of source_table the metrics are computed on. e.g. year=2024/month=1/day=1.',
    partition_ts        TIMESTAMP               COMMENT 'A Timestamp representation of partition_id.'
)
USING iceberg TBLPROPERTIES ('format-version'='2')
PARTITIONED BY (
    years(`partition_ts`),
    `source_table`          -- source_table partitioning helps concurrent writers not step on each other
)
LOCATION '${location}'
;