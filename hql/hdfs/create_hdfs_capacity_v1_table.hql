-- HDFS capacity, one row per week, from `hdfs dfs -df /`.
-- Filled by the insert_hdfs_capacity task of the hdfs_usage_weekly DAG in airflow-dags.
-- The HDFS growth dashboard reads this table to compute the runway (T438056).
--
-- The values come from /wmf/data/raw/hdfs/fsimage/df_YYYY-MM-DD.txt. The namenode backup
-- timer writes this file right after it fetches the fsimage of the same day. A rerun gives
-- the same values.
--
-- Usage:
--     sudo -u analytics kerberos-run-command analytics spark35-sql \
--         -f create_hdfs_capacity_v1_table.hql \
--         -d table_name=wmf_data_ops.hdfs_capacity_v1 \
--         -d base_directory=hdfs://analytics-hadoop/wmf/data/hdfs/hdfs_capacity_v1

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
    `capacity_bytes`  BIGINT COMMENT 'Configured capacity of HDFS, in bytes (Size in hdfs dfs -df)',
    `used_bytes`      BIGINT COMMENT 'Bytes used by HDFS files, replication included (Used in hdfs dfs -df)',
    `remaining_bytes` BIGINT COMMENT 'Bytes free for HDFS files (Available in hdfs dfs -df)',
    `day`             DATE   COMMENT 'Day of the fsimage of the same weekly run'
)
USING ICEBERG
COMMENT 'HDFS capacity, used and remaining bytes, one row per week.'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${base_directory}'
;
