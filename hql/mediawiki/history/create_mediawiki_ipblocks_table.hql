-- Creates table statement for raw mediawiki_ipblocks table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_ipblocks_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_ipblocks`(
  `ipb_id`                  bigint  COMMENT 'Primary key, introduced for privacy.',
  `ipb_address`             string  COMMENT 'Blocked IP address in dotted-quad form or user name.',
  `ipb_user`                bigint  COMMENT 'Blocked user ID or 0 for IP blocks.',
  `ipb_by`                  bigint  COMMENT 'User ID of the administrator who made the block.',
  `ipb_by_text`             string  COMMENT 'Text username of the administrator who made the block.',
  `ipb_reason`              string  COMMENT 'Reason for the block given by the administrator.',
  `ipb_timestamp`           string  COMMENT 'Creation (or refresh) date in standard YMDHMS form.',
  `ipb_auto`                boolean COMMENT 'Indicates that the IP address was blocked because a blocked user accessed a page through it. If this is 1, ipb_address will be hidden.',
  `ipb_anon_only`           boolean COMMENT 'If set to 1, the block only applies to logged out users.',
  `ipb_create_account`      boolean COMMENT 'Prevents account creation from matching IP addresses.',
  `ipb_enable_autoblock`    boolean COMMENT 'Enables autoblock on the block.',
  `ipb_expiry`              string  COMMENT 'Expiry time set by the administrator at the time of the block. A standard timestamp or the string \'infinity\'',
  `ipb_range_start`         string  COMMENT 'The first IP in an IP range block.',
  `ipb_range_end`           string  COMMENT 'The last IP in an IP range block.',
  `ipb_deleted`             boolean COMMENT 'Allows the entry to be flagged, hiding it from users and sysops.',
  `ipb_block_email`         boolean COMMENT 'Prevents the user from accessing Special:Emailuser',
  `ipb_allow_usertalk`      boolean COMMENT 'Prevents a blocked user from editing their talk page. A value of 1 means that the user is not allowed to edit his user talk page. (Thus, a less confusing name would have been ipb_block_usertalk).',
  `ipb_parent_block_id`     bigint  COMMENT 'ID of the block that caused this block to exist. Autoblocks set this to the original block so that the original block being deleted also deletes the autoblocks.',
  `ipb_by_actor`            bigint  COMMENT 'This is a foreign key to actor_id in the actor table.',
  `ipb_reason_id`           bigint  COMMENT 'This is a foreign key to comment_id in the comment table.'

)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Ipblocks_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/ipblocks'
;
