-- Creates table statement for raw mediawiki_private_cu_log table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_mediawiki_private_cu_log_table.hql  \
--          --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_private_cu_log` (
  `cul_id`                      bigint      COMMENT 'Primary key, used to uniquely identify a checkuser log entry',
  `cul_timestamp`               string      COMMENT 'mediawiki-formatted timestamp of this event.',
  `cul_user`                    int         COMMENT 'user_id, the id of the user performing the check.',
  `cul_user_text`               string      COMMENT 'The username or the IP address of the user performing the check.',
  `cul_actor`                   bigint      COMMENT 'This is a foreign key to actor_id in the actor table, corresponding to the checkuser who performed the check.',
  `cul_reason`                  string      COMMENT 'The reason given for the check. Comparable to rev_comment.',
  `cul_reason_id`               bigint      COMMENT 'ID to a comment table row that has the reason for the check. This is a foreign key to comment_id in the comment table.',
  `cul_reason_plaintext_id`     bigint      COMMENT 'ID to a comment table row that has the reason for the check but converted to plaintext.',
  `cul_type`                    string      COMMENT 'see https://www.mediawiki.org/wiki/Extension:CheckUser/cu_log_table#cul_type',
  `cul_target_id`               int         COMMENT 'The id of the user who was checked. 0 for checks on IP addresses or ranges. This is a reference into the user table.',
  `cul_target_text`             string      COMMENT 'user_name of the user who was checked',
  `cul_target_hex`              string      COMMENT 'If the target was an IP address, this contains the hexadecimal form of the IP.',
  `cul_range_start`             string      COMMENT 'If the target was an IP range, this field contain the start, in hexadecimal form.',
  `cul_range_end`               string      COMMENT 'If the target was an IP range, this field contain the end, in hexadecimal form.'
)
COMMENT
  'NOTE: This table has private IP data in it.  It should be truncated so no data older than 90 days is available.  See up to date schema documentation for the source table at https://www.mediawiki.org/wiki/Extension:CheckUser/cu_log_table'
PARTITIONED BY (
  `month` string COMMENT 'The month in YYYY-MM format, all cu_log events from that month are imported',
  `wiki_db` string COMMENT 'The wiki_db project'
)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/cu_log'
;
