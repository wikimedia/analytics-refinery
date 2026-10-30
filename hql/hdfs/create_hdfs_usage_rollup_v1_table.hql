-- HDFS directories at path depth 2 (for example /wmf) to 5 (for example /wmf/data/wmf_mediawiki/mediawiki_history_incremental_v1),
-- one row per directory and day.
-- Built from wmf_data_ops.hdfs_usage_v2 by compute_hdfs_usage_rollup_v1.hql.
-- The HDFS growth dashboard reads this table (T438056).
--
-- The rows of depth 2 (/wmf, /user, ...) add up to the cluster total.
--
-- The table holds the directory names under /user/* down to depth 5. Like hdfs_usage_v2,
-- the table directory must be analytics:analytics-admins 750. Create it before the table:
--     hdfs dfs -mkdir -p /wmf/data/hdfs/hdfs_usage_rollup_v1
--     hdfs dfs -chown analytics:analytics-admins /wmf/data/hdfs/hdfs_usage_rollup_v1
--     hdfs dfs -chmod 750 /wmf/data/hdfs/hdfs_usage_rollup_v1
--
-- Usage:
--     sudo -u analytics kerberos-run-command analytics spark35-sql \
--         -f create_hdfs_usage_rollup_v1_table.hql \
--         -d table_name=wmf_data_ops.hdfs_usage_rollup_v1 \
--         -d base_directory=hdfs://analytics-hadoop/wmf/data/hdfs/hdfs_usage_rollup_v1

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
    `path`                   STRING COMMENT 'Path of the directory',
    `path_depth`             INT    COMMENT 'Path depth in the folder hierarchy (1 is the root path /). From 2 to 5.',
    `parent_path`            STRING COMMENT 'Path of the parent directory',
    `user`                   STRING COMMENT 'HDFS owner',
    `group`                  STRING COMMENT 'HDFS group',
    `files_count`            BIGINT COMMENT 'Number of files in the directory, recursively. 0 if the directory has no files.',
    `blocks_size`            BIGINT COMMENT 'Sum of the sizes of all files in the directory, recursively, in bytes, before replication',
    `replicated_blocks_size` BIGINT COMMENT 'Same as blocks_size, multiplied by the replication factor of each file, in bytes. This is the size on disk.',
    `average_file_size`      BIGINT COMMENT 'blocks_size / files_count, in bytes. NULL if the directory has no files.',
    `day`                    DATE   COMMENT 'Day the namenode wrote the fsimage. Partition key.'
)
USING ICEBERG
PARTITIONED BY (`day`)
COMMENT 'HDFS directories at path depth 2 (for example /wmf) to 5 (for example /wmf/data/wmf_mediawiki/mediawiki_history_incremental_v1), with their sizes and file counts, for each weekly fsimage.'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${base_directory}'
;

ALTER TABLE ${table_name} WRITE ORDERED BY `path_depth`, `path`;
