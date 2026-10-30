-- All HDFS inodes (directories, files, ...) of a weekly fsimage, one row per inode and day.
-- Written by HdfsXMLFsImageConverter (analytics/refinery/source, refinery-job-35).
-- Replaces wmf.hdfs_usage (T438056).
--
-- The dataset contains internal data about HDFS, including the list of files in the `/user/*` folders.
-- To keep the same privacy level as between HDFS users, the table directory must be
-- analytics:analytics-admins 750. Create the directory before the table:
--     hdfs dfs -mkdir -p /wmf/data/hdfs/hdfs_usage_v2
--     hdfs dfs -chown analytics:analytics-admins /wmf/data/hdfs/hdfs_usage_v2
--     hdfs dfs -chmod 750 /wmf/data/hdfs/hdfs_usage_v2
--
-- The main consumer of this table is the HDFS Usage Superset dashboard at https://superset.wikimedia.org/superset/dashboard/409.
-- It filters on type and path_depth, thus the sort order keeps those rows together, so Parquet min/max statistics skip most files.
--
-- Usage:
--     sudo -u analytics kerberos-run-command analytics spark35-sql \
--         -f create_hdfs_usage_v2_table.hql \
--         -d table_name=wmf_data_ops.hdfs_usage_v2 \
--         -d base_directory=hdfs://analytics-hadoop/wmf/data/hdfs/hdfs_usage_v2

CREATE EXTERNAL TABLE IF NOT EXISTS ${table_name} (
    `id`                       BIGINT COMMENT 'Unique identification number of the inode',
    `parent_id`                BIGINT COMMENT 'Self reference to the parent inode id',
    `type`                     STRING COMMENT 'Type of inode: FILE or DIRECTORY',
    `name`                     STRING COMMENT 'Name of the directory or the file',
    `path`                     STRING COMMENT 'Path of the file or directory (includes the name column at the end)',
    `path_depth`               INT    COMMENT 'Path depth in the folder hierarchy (beginning at 1. 1 is the root path inode /)',
    `replication`              INT    COMMENT 'Replication factor',
    `mtime`                    BIGINT COMMENT 'Date of last modification, in milliseconds since the epoch',
    `atime`                    BIGINT COMMENT 'Date of last access, in milliseconds since the epoch',
    `preferred_block_size`     BIGINT COMMENT 'Preferred size of the block, in bytes',
    `blocks_count`             BIGINT COMMENT 'Number of blocks used by the file, or the files of the directory recursively',
    `blocks_size`              BIGINT COMMENT 'Sum of bytes used by all blocks. For 1 file, or for all files in directory recursively',
    `replicated_blocks_size`   BIGINT COMMENT 'Sum of bytes used by all blocks including their replication',
    `reserved_size`            BIGINT COMMENT 'Reserved size of the file/folder, in bytes. (Sum of preferred_block_size * blocks_count)',
    `replicated_reserved_size` BIGINT COMMENT 'Same as reserved_size, but multiplying by the replication factor',
    `files_count`              BIGINT COMMENT 'Number of files in directory, recursively',
    `average_file_size`        BIGINT COMMENT 'Average size of files in the directory, in bytes',
    `user`                     STRING COMMENT 'HDFS owner',
    `group`                    STRING COMMENT 'HDFS group',
    `permission`               STRING COMMENT 'HDFS permission, in octal (for example 0750)',
    `day`                      DATE   COMMENT 'Day the namenode wrote the fsimage. Partition key.'
)
USING ICEBERG
PARTITIONED BY (`day`)
COMMENT 'All HDFS file system inodes (directories, files, ...), with their properties, for each weekly fsimage.'
TBLPROPERTIES (
    'format-version' = '2',
    'write.parquet.compression-codec' = 'zstd'
)
LOCATION '${base_directory}'
;

ALTER TABLE ${table_name} WRITE ORDERED BY `type`, `path_depth`, `path`;
