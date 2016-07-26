DROP TABLE `wmf.mediawiki_user_groups`
;
CREATE EXTERNAL TABLE `wmf.mediawiki_user_groups`(
  `ug_user`                     bigint      COMMENT 'This field links to a given user\'s user_id. It is a foreign key used to link accounts with their assigned privileges.',
  `ug_group`                    string      COMMENT 'This field stores the user\'s permissions, which are stored as groups. At runtime, $wgGroupPermissions will associate group keys with particular permissions\; a user will have the combined permissions of any group they\'re explicitly in, plus the implicit \'*\' and \'user\' groups. Example ug_group values: \'bot\', \'bureaucrat\', \'sysop\'.  There is one row for each (explicit) group the user is in.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:User_groups_table'
PARTITIONED BY
  (`wiki_db` string)
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/user_groups'
;

-- find all partitons, per http://stackoverflow.com/a/35834372/180664
MSCK REPAIR TABLE `wmf.mediawiki_user_groups`
;
