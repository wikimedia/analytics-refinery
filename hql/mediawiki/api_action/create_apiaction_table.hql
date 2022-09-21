-- Create apiaction table
--
-- MediaWiki Action API (api.php) requests
--
-- NOTE: the schema is embedded in the CREATE statement.
-- This is not ideal because:
--  * We'll have to re-create the table when we change the schema
--  * If schema grows a bit it might go over the length hive has allotted to
--    this field and thus table creation will fail.
--
-- See https://phabricator.wikimedia.org/T118155 for more details
--
-- TODO: review this script and use avro.schema.url once we have
--       an official repo in hdfs for schemas.
--
-- Parameters:
--     None
-- Usage:
--     hive -f create_apiaction_table.hql --database wmf_raw
--

CREATE EXTERNAL TABLE apiaction
PARTITIONED BY (
  `year` string,
  `month` string,
  `day` string,
  `hour` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/mediawiki_ApiAction/hourly'
TBLPROPERTIES (
'avro.schema.literal'='{"type":"record","name":"ApiAction","namespace":"org.wikimedia.analytics.schemas","fields":[{"name":"ts","type":"int","default":0},{"name":"ip","type":"string","default":""},{"name":"userAgent","type":"string","default":""},{"name":"wiki","type":"string","default":""},{"name":"timeSpentBackend","type":"int","default":-1},{"name":"hadError","type":"boolean","default":false},{"name":"errorCodes","type":{"type":"array","items":"string"},"default":[]},{"name":"params","type":{"type":"map","values":"string"},"default":{}}]}'
)
;
