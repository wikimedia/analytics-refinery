-- Creates table statement for raw mediawiki_redirect table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_redirect_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_redirect`(
  `rd_from`             bigint      COMMENT 'Contains the page_id of the source page',
  `rd_namespace`        int         COMMENT 'Contains the number of the target''s namespace',
  `rd_title`            string      COMMENT 'Contains the sanitized title of the target page. It is stored as text, with spaces replaced by underscores',
  `rd_interwiki`        string      COMMENT 'MediaWiki version:  ≥ 1.16 - This field is not empty only if an interwiki prefix is used: #REDIRECT [[prefix:…]] (a prefix such as "w:" for Wikipedia, or an interlanguage link prefix such as "nl:" for Dutch, but not both). In this case a redirect is not visible in Special:WhatLinksHere (even if the target lies in the same wiki), rd_namespace is always 0 and rd_title may contain a possible namespace prefix, but rd_fragment may be non-NULL',
  `rd_fragment`         string      COMMENT 'Contains the target''s fragment ID if present (see also bugzilla:218), otherwise is NULL'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Redirect_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/redirect'
;
