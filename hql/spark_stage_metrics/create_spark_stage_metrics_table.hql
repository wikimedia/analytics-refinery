-- Per-stage metrics of Spark applications, read from Spark event logs by
-- SparkEventLogMetrics (analytics/refinery/source, refinery-job-35).
--
-- One row per stage attempt, with the job and the application that ran it
-- denormalised onto it. A stage Spark retried appears once per attempt.
--
-- Units are carried in the column names because Spark does not report them all
-- in the same one: every CPU time and the shuffle write time are nanoseconds,
-- every other duration is milliseconds. Values are passed through as Spark
-- reports them rather than normalised, so figures here match the Spark UI.
--
-- No column is declared NOT NULL, including the partition keys. Spark's encoder
-- marks every non-primitive field nullable -- application_id, application_start
-- and stage_result among them -- and an Iceberg append of a nullable field into
-- a required column is rejected. The columns the job always sets are marked
-- "Always set" in their comment instead.
--
-- Usage:
--     spark3-sql -f create_spark_stage_metrics_table.hql \
--                -d table_name=wmf_data_ops.spark_stage_metrics \
--                -d base_directory=hdfs://analytics-hadoop/wmf/data/wmf_data_ops/spark_stage_metrics


CREATE TABLE IF NOT EXISTS ${table_name} (
    `airflow_id`                        STRING    COMMENT 'Airflow task that launched the application, as {DAG_ID}_{TASK_ID}, from the spark.wmf.metrics.airflow_id property recorded in the event log. Partition key. Null only when the job ran with require_airflow_id=false and the application was not launched by Airflow.',
    `application_id`                    STRING    COMMENT 'YARN application id. Unique per application, and the key the job uses to avoid reading an event log twice. Always set.',
    `application_attempt_id`            STRING    COMMENT 'Which attempt at the application this is, when YARN restarted the driver.',
    `application_start`                 TIMESTAMP COMMENT 'When the application started. Partition key, and always set: an event log whose start was lost produces no rows at all.',
    `application_end`                   TIMESTAMP COMMENT 'When the application ended. Null when the driver was killed before it could report an end.',
    `application_result`                STRING    COMMENT 'succeeded, failed, or incomplete when the application never reported an end. Derived, not reported: Spark logs no application success flag, so this says the application ended and no job in it failed. An application that catches a job failure and exits cleanly reads as failed here. Always set.',
    `application_name`                  STRING    COMMENT 'spark.app.name of the application.',
    `spark_user`                        STRING    COMMENT 'User the application ran as.',
    `spark_version`                     STRING    COMMENT 'Spark version the application ran on, from the event log header.',
    `spark_master`                      STRING    COMMENT 'spark.master of the application, e.g. yarn.',
    `spark_config`                      MAP<STRING, STRING> COMMENT 'The settings the application ran with, so a change in how a job is configured can be read next to the change in what it cost. An allow-listed subset of the logged Spark properties -- executor and driver sizing, dynamic allocation, shuffle partitions, adaptive execution and spark.wmf.* -- because the full map holds whatever the submitter passed on the command line. See SparkStageMetricsCollector.ConfigAllowList. Empty, not null, for an application that set none of them.',
    `job_id`                            INT       COMMENT 'Job that submitted the stage attempt. Null when the event log never attributed it to one.',
    `job_group`                         STRING    COMMENT 'spark.jobGroup.id of that job, when it set one.',
    `job_result`                        STRING    COMMENT 'succeeded, failed, or incomplete when the job had not ended.',
    `stage_id`                          INT       COMMENT 'Stage identifier, unique within the application. Always set.',
    `stage_attempt`                     INT       COMMENT 'Which attempt at that stage this row describes, counting from 0. A retried stage appears once per attempt, because the work a failed attempt did is real. Always set.',
    `stage_name`                        STRING    COMMENT 'Spark name for the stage, e.g. save at Refine.scala:120.',
    `stage_result`                      STRING    COMMENT 'succeeded, failed, or incomplete when the stage had not finished. Always set.',
    `stage_submitted`                   TIMESTAMP COMMENT 'When the stage attempt was submitted.',
    `stage_completed`                   TIMESTAMP COMMENT 'When the stage attempt finished. Null when it never did.',
    `stage_duration_ms`                 BIGINT    COMMENT 'Wall clock duration of the stage attempt. Null unless both ends are known.',
    `num_tasks`                         BIGINT    COMMENT 'Tasks the stage attempt planned to run.',
    `tasks_completed`                   BIGINT    COMMENT 'Tasks of the stage attempt that completed.',
    `tasks_failed`                      BIGINT    COMMENT 'Tasks of the stage attempt that failed.',
    `tasks_killed`                      BIGINT    COMMENT 'Tasks of the stage attempt that were killed.',
    `executor_run_time_ms`              BIGINT    COMMENT 'Time the executors spent running the tasks of the stage attempt.',
    `executor_cpu_time_ns`              BIGINT    COMMENT 'CPU time the executors spent running the tasks of the stage attempt. Nanoseconds, as Spark reports it.',
    `executor_deserialize_time_ms`      BIGINT    COMMENT 'Time spent deserializing the tasks of the stage attempt.',
    `executor_deserialize_cpu_time_ns`  BIGINT    COMMENT 'CPU time spent deserializing the tasks of the stage attempt. Nanoseconds, as Spark reports it.',
    `jvm_gc_time_ms`                    BIGINT    COMMENT 'Time the executor JVMs spent in garbage collection while running the tasks of the stage attempt.',
    `result_size_bytes`                 BIGINT    COMMENT 'Serialized size of the results the tasks sent back to the driver.',
    `result_serialization_time_ms`      BIGINT    COMMENT 'Time spent serializing the results of the tasks.',
    `memory_bytes_spilled`              BIGINT    COMMENT 'In-memory bytes spilled by the tasks of the stage attempt.',
    `disk_bytes_spilled`                BIGINT    COMMENT 'On-disk bytes spilled by the tasks of the stage attempt.',
    `peak_execution_memory_bytes`       BIGINT    COMMENT 'Largest peak execution memory reported by any one task of the stage attempt. A maximum, not a sum: summing per-task peaks would not describe anything real.',
    `input_bytes_read`                  BIGINT    COMMENT 'Bytes read by the tasks of the stage attempt.',
    `input_records_read`                BIGINT    COMMENT 'Records read by the tasks of the stage attempt.',
    `output_bytes_written`              BIGINT    COMMENT 'Bytes written by the tasks of the stage attempt.',
    `output_records_written`            BIGINT    COMMENT 'Records written by the tasks of the stage attempt.',
    `shuffle_local_blocks_fetched`      BIGINT    COMMENT 'Shuffle blocks the tasks fetched locally.',
    `shuffle_remote_blocks_fetched`     BIGINT    COMMENT 'Shuffle blocks the tasks fetched remotely.',
    `shuffle_local_bytes_read`          BIGINT    COMMENT 'Shuffle bytes the tasks read locally.',
    `shuffle_remote_bytes_read`         BIGINT    COMMENT 'Shuffle bytes the tasks read remotely.',
    `shuffle_remote_bytes_read_to_disk` BIGINT    COMMENT 'Shuffle bytes the tasks read remotely straight to disk.',
    `shuffle_records_read`              BIGINT    COMMENT 'Shuffle records read by the tasks of the stage attempt.',
    `shuffle_fetch_wait_time_ms`        BIGINT    COMMENT 'Time the tasks spent blocked waiting for shuffle blocks.',
    `shuffle_bytes_written`             BIGINT    COMMENT 'Shuffle bytes written by the tasks of the stage attempt.',
    `shuffle_records_written`           BIGINT    COMMENT 'Shuffle records written by the tasks of the stage attempt.',
    `shuffle_write_time_ns`             BIGINT    COMMENT 'Time the tasks spent writing shuffle data. Nanoseconds, as Spark reports it.'
)
USING ICEBERG
PARTITIONED BY (airflow_id, months(application_start))
COMMENT 'Per-stage metrics of Spark applications, derived from Spark event logs.'
TBLPROPERTIES (
    'format-version' = '2',
    'write.delete.mode' = 'copy-on-write',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${base_directory}'
;
