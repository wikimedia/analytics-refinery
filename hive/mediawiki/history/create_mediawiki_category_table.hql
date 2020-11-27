-- Creates table statement for raw mediawiki_category table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_category_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_category`(
  `cat_id`              bigint  COMMENT 'Primary key',
  `cat_title`           string  COMMENT 'Name of the category, in the same form as page.page_title (with underscores). If there is a category page corresponding to this category, by definition, it has this name (in the Category namespace).',
  `cat_pages`           int     COMMENT 'Number of pages in the category. This number includes the number of subcategories and the number of files.',
  `cat_subcats`         int     COMMENT 'Number of sub-categories in the category.',
  `cat_files`           int     COMMENT 'Number of files (i.e. Image: namespace members) in the category.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Category_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/category'
;
