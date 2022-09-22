-- Creates table statement for raw mediawiki_project_namespace_map table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_project_namespace_map_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_project_namespace_map`(
    `hostname`                  string  COMMENT 'Canonical URL for the project, for example ja.wikipedia.org',
    `dbname`                    string  COMMENT 'Database name for the project, for example jawiki',
    `namespace`                 int     COMMENT 'for example 0, 100, etc.',
    `namespace_canonical_name`  string  COMMENT 'the english prefix if exists, otherwise the localized prefix',
    `namespace_localized_name`  string  COMMENT 'the localized prefix',
    `namespace_is_content`      int     COMMENT 'Whether this namespace is a content namespace'
)
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/project_namespace_map'
;
