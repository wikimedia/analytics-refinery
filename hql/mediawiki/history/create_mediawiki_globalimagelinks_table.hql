-- Create table statement for raw mediawiki_globalimagelinks table.
--
-- globalimagelinks table on commonswiki comes from GlobalUsage extension.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_globalimagelinks_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_globalimagelinks`(
  `gil_wiki`                  string  COMMENT 'The wiki_db of the consuming wiki. EXAMPLE: enwiki',
  `gil_page`                  bigint  COMMENT 'page_id on the consuming wiki where the file is inlined. EXAMPLE: 3409375',
  `gil_to`                    string  COMMENT 'Filename (dbkey form, no File: prefix) on Commons. EXAMPLE: Stop_hand.png',
  `gil_page_namespace_id`     int     COMMENT 'Numeric namespace ID on the consuming wiki. EXAMPLE: 0',
  `gil_page_namespace`        string  COMMENT 'String namespace name on the consuming wiki. Empty string for ns0. EXAMPLE: User_talk',
  `gil_page_title`            string  COMMENT 'Page title (dbkey form) on the consuming wiki. EXAMPLE: 2001_World_Series'
)
COMMENT
  'See https://www.mediawiki.org/wiki/Extension:GlobalUsage and https://www.mediawiki.org/wiki/Extension:GlobalUsage/globalimagelinks_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular cloud imports).',
  `wiki_db` string COMMENT 'The wiki_db project; commonswiki in practice as of 2026-05-28; maybe more some day.')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/globalimagelinks'
;
