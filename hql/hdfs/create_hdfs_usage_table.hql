-- Creates table statement for hdfs_usage table.
--
-- The dataset contains internal data about HDFS, including the list of files in the `/user/*` folders.
-- In order to replicate an equivalent level of privacy we have between HDFS users,
-- we set this dataset HDFS permission to: analytics:analytics-admins:750
--
-- On HDFS, when creating a file or a directory, the default permission is the one of the parent folder.
-- So, in our case, we need to set the permission of the parent folder at creation:
--     hdfs dfs -mkdir -p /wmf/data/hdfs/usage
--     hdfs dfs -chown -R analytics:analytics-admins /wmf/data/hdfs/usage
--     hdfs dfs -chmod -R 750 /wmf/data/hdfs/usage
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql \
--         -f create_hdfs_usage_table.hql \
--         --database wmf

CREATE EXTERNAL TABLE IF NOT EXISTS hdfs_usage (
    id                       bigint  COMMENT 'Unique identification number of the inode',
    parent_id                bigint  COMMENT 'Self reference to the parent inode id',
    type                     string  COMMENT 'Type of inode: file, directory, link, ...',
    name                     string  COMMENT 'Name of the directory or the file',
    `path`                   string  COMMENT 'Path of the file or directory (includes the name column at the end)',
    path_depth               tinyint COMMENT 'Path depth in the folder hierarchy (beginning at 1. 1 is the root path inode /)',
    `replication`            tinyint COMMENT 'Replication factor',
    mtime                    bigint  COMMENT 'Date of last modification',
    atime                    bigint  COMMENT 'Date of last access',
    preferred_block_size     integer COMMENT 'Preferred size of the block',
    blocks_count             bigint  COMMENT 'Number of blocks used by the file, or the files of the directory recursively',
    blocks_size              bigint  COMMENT 'Sum of bytes used by all blocks. For 1 file, or for all files in directory recursively',
    replicated_blocks_size   bigint  COMMENT 'Sum of bytes used by all blocks including their replication',
    reserved_size            bigint  COMMENT 'Reserved size of the file/folder, in bytes. (Sum of preferred_block_size * blocks_count)',
    replicated_reserved_size bigint  COMMENT 'Same as reserved_size, but multiplying by the replication factor',
    files_count              bigint  COMMENT 'Number of files in directory, recursively',
    average_file_size        bigint  COMMENT 'Average size of files in the directory, in bytes',
    `user`                   string  COMMENT 'HDFS owner',
    `group`                  string  COMMENT 'HDFS group',
    permission               string  COMMENT 'HDFS permission'
)
COMMENT "List all HDFS file system inodes (directories, files,...), including their properties."
PARTITIONED BY (
    snapshot string COMMENT 'Beginning of the snapshot time period (YYYY-MM-DD)'
)
STORED AS PARQUET
LOCATION '/wmf/data/hdfs/usage' ;
