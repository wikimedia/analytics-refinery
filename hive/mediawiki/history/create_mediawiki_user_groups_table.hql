-- Creates table statement for raw mediawiki_user_groups table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_user_groups_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_user_groups`(
  `ug_user`                     bigint      COMMENT 'This field links to a given user\'s user_id. It is a foreign key used to link accounts with their assigned privileges.',
  `ug_group`                    string      COMMENT 'This field stores the user\'s permissions, which are stored as groups. At runtime, $wgGroupPermissions will associate group keys with particular permissions\; a user will have the combined permissions of any group they\'re explicitly in, plus the implicit \'*\' and \'user\' groups. Example ug_group values: \'bot\', \'bureaucrat\', \'sysop\'.  There is one row for each (explicit) group the user is in.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:User_groups_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/user_groups'
;
