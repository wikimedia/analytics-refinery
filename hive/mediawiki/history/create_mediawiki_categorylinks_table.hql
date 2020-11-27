-- Creates table statement for raw mediawiki_categorylinks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_categorylinks_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_categorylinks`(
  `cl_from`             bigint  COMMENT 'Stores the page.page_id of the article where the link was placed.',
  `cl_to`               string  COMMENT 'Stores the name (excluding namespace prefix) of the desired category. Spaces are replaced by underscores (_)',
  `cl_sortkey`          string  COMMENT 'Stores the title by which the page should be sorted in a category list. This is the binary sortkey, that depending on $wgCategoryCollation may or may not be readable by a human (but should sort in correct order when comparing as a byte string), and is not valid UTF-8 whenever the database truncates the sortkey in the middle of a multi-byte sequence.',
  `cl_timestamp`        string  COMMENT 'Stores the time at which that link was last updated in the table.',
  `cl_sortkey_prefix`   string  COMMENT 'This is either the empty string if a page is using the default sortkey (aka the sortkey is unspecified). Otherwise it is the human readable version of cl_sortkey. Needed mostly so that cl_sortkey can be easily updated in certain situations without re-parsing the entire page. More recently added values are valid UTF-8 (see change 449280 on Gerrit).',
  `cl_collation`        string  COMMENT 'What collation is in use. Used so that if the collation changes, the updateCollation.php script knows what rows need to be fixed in db.',
  `cl_type`             string  COMMENT 'What type of page is this (file, subcat (subcategory) or page (normal page)). Used so that the different sections on a category page can be paged independently in an efficient manner.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Categorylinks_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/categorylinks'
;
