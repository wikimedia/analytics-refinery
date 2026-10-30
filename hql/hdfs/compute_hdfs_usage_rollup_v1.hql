-- Fill one day of wmf_data_ops.hdfs_usage_rollup_v1 from wmf_data_ops.hdfs_usage_v2.
-- Keeps the directories at path depth 2 (for example /wmf) to 5 (for example /wmf/data/wmf_mediawiki/mediawiki_history_incremental_v1).
-- A rerun replaces the rows of the day.
--
-- Parameters:
--     source_table      -- Fully qualified name of the hdfs_usage_v2 table
--     destination_table -- Fully qualified name of the hdfs_usage_rollup_v1 table
--     day               -- Day of the fsimage, YYYY-MM-DD
--
-- Usage:
--     spark35-sql -f compute_hdfs_usage_rollup_v1.hql \
--         -d source_table=wmf_data_ops.hdfs_usage_v2 \
--         -d destination_table=wmf_data_ops.hdfs_usage_rollup_v1 \
--         -d day=2026-10-19

DELETE FROM ${destination_table}
WHERE `day` = DATE('${day}');

INSERT INTO TABLE ${destination_table}
SELECT
    `path`,
    `path_depth`,
    CASE WHEN `path_depth` = 2 THEN '/'
         ELSE regexp_extract(`path`, '^(.*)/[^/]+$', 1) END        AS `parent_path`,
    `user`,
    `group`,
    COALESCE(`files_count`, 0)                                    AS `files_count`,
    `blocks_size`,
    `replicated_blocks_size`,
    `average_file_size`,
    `day`
FROM ${source_table}
WHERE `day` = DATE('${day}')
  AND `type` = 'DIRECTORY'
  AND `path_depth` BETWEEN 2 AND 5;
