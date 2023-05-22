-- Creates table statement for raw mediawiki_externallinks_old table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql \
--       --database wmf_raw \
--       -f create_mediawiki_externallinks_old_table.hql \
--

CREATE EXTERNAL TABLE `mediawiki_externallinks_old`(
  `el_id`               bigint  COMMENT 'The primary key. Presently not used for anything, but will help with online schema changes.',
  `el_from`             bigint  COMMENT 'The page id of the referring wiki page.',
  `el_to`               string  COMMENT 'The actual URL itself. It is passed to the browser.',
  `el_index`            string  COMMENT 'This is the same URL as el_to search-optimized: username and password information is stripped, and the other components are reversed for faster searching, so http://user:password@sub.example.com/page.html becomes http://com.example.sub./page.html, which allows searches of the form \'Show all links pointing to *.example.com\'.',
  `el_index_60`         string  COMMENT 'This is el_index truncated to 60 bytes to allow for sortable queries that aren\'t supported by a partial index.'
)
COMMENT
  'Deprecated - See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Externallinks_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)',
  `wiki_db` string COMMENT 'The wiki_db project')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/externallinks_old'
;
