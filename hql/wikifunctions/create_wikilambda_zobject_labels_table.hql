-- Creates table statement for raw wikilambda_zobject_labels table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_wikilambda_zobject_labels_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `wikilambda_zobject_labels`(
  `wlzl_id`                   bigint  COMMENT 'Unique ID for index purposes',
  `wlzl_zobject_zid`          string  COMMENT 'The ZID of the ZObject',
  `wlzl_type`                 string  COMMENT 'The ZObject type',
  `wlzl_language`             string  COMMENT 'The language code of the label',
  `wlzl_label_primary`        boolean COMMENT 'Whether the entry is a primary label or an alias',
  `wlzl_return_type`          string  COMMENT 'The return type if the ZObject is a function or function call'
)
COMMENT
  'Info about labels of Wikifunctions ZObjects'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/wikilambda_zobject_labels'
;
