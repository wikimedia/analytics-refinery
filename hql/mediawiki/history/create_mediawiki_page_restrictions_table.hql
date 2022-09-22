-- Creates table statement for raw mediawiki_page_restrictions table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_page_restrictions_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_page_restrictions`(
  `pr_id`               bigint      COMMENT 'This is the primary key for the table, and is used to identify a particular row in the table, along with wiki.',
  `pr_page`             bigint      COMMENT 'This field contains a reference to page_id, which works as the foreign key for this table.',
  `pr_type`             string      COMMENT 'The type of protection (whether it applies to edits, page moves, or similar) is stored in this field.',
  `pr_level`            string      COMMENT 'This column describes the level of protection for the page, full protection for sysop-only pages, semi-protection for autoconfirmed users, or any other levels.',
  `pr_cascade`          int         COMMENT 'This field determines whether cascading protection (meaning that all transcluded templates and images on the page will be protected as well).',
  `pr_user`             bigint      COMMENT 'This field is reserved to support a future per-user edit restriction system.',
  `pr_expiry`           string      COMMENT 'This field contains the timestamp for pages whose protection has a set expiration date, and has a format similar to the expiry time in the Ipblocks table. Rows that contain a null value in this column are considered to be protected indefinitely.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Page_restrictions_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/page_restrictions'
;
