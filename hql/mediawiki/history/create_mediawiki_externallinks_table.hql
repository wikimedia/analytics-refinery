-- Creates table statement for raw mediawiki_externallinks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql \
--       --database wmf_raw \
--       -f create_mediawiki_externallinks_table.hql \
--

CREATE EXTERNAL TABLE `mediawiki_externallinks`(
    `el_id`              bigint COMMENT 'The primary key. Presently not used for anything, but will help with online schema changes.',
    `el_from`            bigint COMMENT 'The page id of the referring wiki page.',
    `el_to_domain_index` string COMMENT 'This is the base URL search-optimized: username and password information is stripped, and the other components are reversed for faster searching. It allows searches of the form: Show all links pointing to *.example.com. E.g., http://user:password@sub.example.com/page.html becomes http://com.example.sub., http://org.iau.www.',
    `el_to_path`         string COMMENT 'This is the URL path. e.g. /public_press/themes/naming/#minorplanets'
)
COMMENT 'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Externallinks_table'
PARTITIONED BY (
    `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)',
    `wiki_db` string COMMENT 'The wiki_db project'
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION 'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/externallinks'
;
