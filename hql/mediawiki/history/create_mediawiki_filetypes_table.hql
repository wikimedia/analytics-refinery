-- Create table statement for raw mediawiki_filetypes table.
--
-- Usage
--     spark-sql -f create_mediawiki_filetypes_table.hql --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_filetypes`(
  `ft_id`          bigint  COMMENT 'Prmary key.',
  `ft_media_type`  string  COMMENT 'Media type as defined by the MEDIATYPE_xxx constants.',
  `ft_major_mime`  string  COMMENT 'Major part of a MIME media type as defined by IANA, see https://www.iana.org/assignments/media-types/. For chemical types, which are defined by the ACS, see http://dx.doi.org/10.1021/ci9803233.',
  `ft_minor_mime`  string  COMMENT 'Minor part of a MIME media type as defined by IANA the minor parts are not required to adhere to any standard but should be consistent throughout the database see https://www.iana.org/assignments/media-types/.'
)
COMMENT 'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Filetypes_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning snapshot label, i.e. 2025-04.',
  `wiki_db` string COMMENT 'The wiki database code, i.e. commonswiki.'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/filetypes'
;
