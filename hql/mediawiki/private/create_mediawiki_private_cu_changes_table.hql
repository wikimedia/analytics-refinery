-- Creates table statement for raw mediawiki_private_cu_changes table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_private_cu_changes.hql  \
--          --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_private_cu_changes` (
  `cuc_id`              bigint      COMMENT 'Primary key, artificial',
  `cuc_namespace`       bigint      COMMENT 'When pages are renamed, their RC entries do _not_ change.',
  `cuc_title`           string      COMMENT 'When pages are renamed, their RC entries do _not_ change.',
  `cuc_user`            bigint      COMMENT 'user_id, rev_user, the id of the user performing the action',
  `cuc_user_text`       string      COMMENT 'user_text, rev_user_text, the name of the user at the time',
  `cuc_actiontext`      string      COMMENT 'Unknown, undocumented',
  `cuc_comment`         string      COMMENT 'The revision comment',
  `cuc_minor`           boolean     COMMENT 'Whether this was a minor edit',
  `cuc_page_id`         bigint      COMMENT 'The id of the page being edited',
  `cuc_this_oldid`      bigint      COMMENT 'rev_id of the revision represented by this change',
  `cuc_last_oldid`      bigint      COMMENT 'rev_id of the previous revision on this page',
  `cuc_type`            int         COMMENT 'see https://www.mediawiki.org/wiki/Manual:Recentchanges_table#rc_type',
  `cuc_timestamp`       string      COMMENT 'mediawiki-formatted timestamp of this event',
  `cuc_ip`              string      COMMENT 'clear-text IP address of the user responsible for this event',
  `cuc_agent`           string      COMMENT 'clear-text user agent of the user responsible for this event'
)
COMMENT
  'NOTE: This table has private IP data in it.  It should be truncated so no data older than 90 days is available.  See up to date schema documentation for the source table at https://www.mediawiki.org/wiki/Extension:CheckUser/cu_changes_table'
PARTITIONED BY (
  `month` string COMMENT 'The month in YYYY-MM format, all cu_changes events from that month are imported',
  `wiki_db` string COMMENT 'The wiki_db project'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/cu_changes'
;
