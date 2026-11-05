-- Create table statement for the Iceberg maintenance audit table.
--
-- The iceberg_maintenance_rules_* DAGs in airflow-dags write one row for each action on
-- each table, and one row for each table that a rule skipped or ran as a dry run.
--
-- Usage:
--     sudo -u analytics spark3-sql -f create_iceberg_maintenance_audit_table.hql \
--     --database wmf_data_ops \
--     -d location=hdfs://analytics-hadoop/wmf/data/iceberg_maintenance/audit/
--
CREATE EXTERNAL TABLE `iceberg_maintenance_audit` (
    -- what was touched, under which config
    table_name          STRING              COMMENT 'Fully qualified name of the maintained table, e.g. wmf_traffic.stg_pageview_actor.',
    config_source       STRING              COMMENT 'Config that selected the table, e.g. maintenance_rules.',
    config_key          STRING              COMMENT 'Name of the rule that selected the table.',
    config_json         STRING              COMMENT 'The rule as applied, with defaults resolved, as JSON.',
    -- what was done
    action              STRING              COMMENT 'resolve, delete, expire_snapshots, remove_orphan_files, rewrite_manifests or rewrite_data_files.',
    status              STRING              COMMENT 'succeeded, skipped or dry_run.',
    skip_reason         STRING              COMMENT 'Set when status = skipped: in_datasets_yaml, not_iceberg, no_time_column, ambiguous_time_column, match_limit or floor_guard.',
    error_message       STRING              COMMENT 'Reserved for failed actions. A failed action currently fails its job and writes no row.',
    -- retention details (action = delete only)
    time_column         STRING              COMMENT 'Resolved time column.',
    retain              STRING              COMMENT 'Retention as configured, e.g. 3 months.',
    cutoff_ts           TIMESTAMP           COMMENT 'Rows with time_column < cutoff_ts were deleted.',
    predicate           STRING              COMMENT 'The rendered WHERE clause, exactly as executed.',
    -- effect (NULL for resolve and dry_run rows)
    snapshot_id_before  BIGINT              COMMENT 'Current snapshot of table_name before the action.',
    snapshot_id_after   BIGINT              COMMENT 'Current snapshot of table_name after the action. Equal to before when nothing was committed.',
    metrics             MAP<STRING,BIGINT>  COMMENT 'Numeric entries of the summary of the snapshot that the action committed, with Iceberg key names, e.g. deleted-records. NULL when nothing was committed.',
    -- when and by which run
    data_interval_end   TIMESTAMP           COMMENT 'Airflow data interval end of the run; base of cutoff_ts.',
    started_ts          TIMESTAMP           COMMENT 'Action start time. For resolve and dry_run rows, the time of the resolution.',
    finished_ts         TIMESTAMP           COMMENT 'Action end time. For resolve and dry_run rows, the time of the resolution.',
    pipeline_run_id     STRING              COMMENT 'Airflow run that wrote the row, as dag_id/run_id.'
)
USING iceberg TBLPROPERTIES ('format-version'='2')
PARTITIONED BY (
    years(`started_ts`),
    `table_name`            -- table_name partitioning helps concurrent writers not step on each other
)
LOCATION '${location}'
;
