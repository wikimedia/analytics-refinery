-- Creates table statement for raw wikilambda_zobject_join table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_wikilambda_zobject_join_table.hql \
--         --database wmf_raw
--
-- NOTE: remember to do this to get the table to see the "latest" snapshot:
--     msck repair table wikilambda_zobject_join;
--

CREATE EXTERNAL TABLE `wikilambda_zobject_join`(
  `wlzo_id`                 bigint  COMMENT 'Unique ID for index purposes',
  `wlzo_main_zid`           string  COMMENT 'The ZID of the main ZObject',
  `wlzo_main_type`          string  COMMENT 'The type of the main ZObject',
  `wlzo_key`                string  COMMENT 'ZKey indicating the relationship between the main and related ZObjects',
  `wlzo_related_zobject`    string  COMMENT 'The related ZObject',
  `wlzo_related_type`       string  COMMENT 'The type of the related ZObject'
)
COMMENT
  'Relationships between ZObjects'
PARTITIONED BY (
  `snapshot` string COMMENT 'In this case, the snapshot will always be set to latest to allow a whole-table import to overwrite the previous data.  This is an experimental approach predicated on small data.  Increased data sizes and retention policies may imply changes in the future.  NOTE: This means that we do not need the usual mediawiki/history/load job as all other similarly sqooped tables need.  Since we always overwrite, there is no need to msck repair table, the partitions stay the same, only the data changes.  It remains to be seen whether this causes any weird behavior.',
  `wiki_db` string COMMENT 'The wiki_db project')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/wikilambda_zobject_join'
;
