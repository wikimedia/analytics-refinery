-- Create table statement for raw mediawiki_file table.
--
-- Usage
--     spark-sql -f create_mediawiki_file_table.hql --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_file`(
  `file_id`       bigint  COMMENT 'Prmary key.',
  `file_name`     string  COMMENT 'Name of the file.',
  `file_latest`   bigint  COMMENT 'Latest revision of the file. Foreign key to the filerevision table.',
  `file_type`     int     COMMENT 'Type of the file. Foreign key to the filetypes table.',
  `file_deleted`  int     COMMENT 'Whether the file is deleted.'
)
COMMENT 'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:File_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning snapshot label, i.e. 2025-04.',
  `wiki_db` string COMMENT 'The wiki database code, i.e. commonswiki.'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/file'
;
