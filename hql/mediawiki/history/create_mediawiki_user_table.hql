-- Creates table statement for raw mediawiki_user table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_user_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_user`(
  `user_id`                     bigint      COMMENT 'the primary key along with wiki, used to uniquely identify a user',
  `user_name`                   string      COMMENT 'Usernames must be unique, and must not be in the form of an IP address. Shouldn\'t allow slashes or case conflicts. See also Manual:$wgInvalidUsernameCharacters. Spaces are allowed, and underscores are converted to spaces (the opposite than with page names).',
  `user_name_binary`            string      COMMENT 'Same as user_name but sqooped unmodified from mediawiki because some user names do not decode properly as utf8 from the varbinary user_name field there.',
  `user_real_name`              string      COMMENT 'stores the user\'s real name (optional) as provided by the user in their "Preferences" section.',
  `user_email`                  string      COMMENT 'Note: email should be restricted, not public info. Same with passwords. \;)',
  `user_touched`                string      COMMENT 'the last time a user made a change on the site, including logins, changes to pages (any namespace), watchlistings, and preference changes.  Note Note: The user_touched time resets when a user is left a talkpage message.',
  `user_registration`           string      COMMENT 'the timestamp of when the user registered. For old users, they may have a value of NULL for this field. The fixUserRegistration.php script can be used to back-populate this field.',
  `user_editcount`              bigint      COMMENT 'Count of edits and edit-like actions.  NOT* intended to be an accurate copy of COUNT(*) WHERE rev_user=user_id.  May contain NULL for old accounts if batch-update scripts haven\'t been run, as well as listing deleted edits and other myriad ways it could be out of sync. Execute the script initEditCount.php to update this table column.  Meant primarily for heuristic checks to give an impression of whether the account has been used much.',
  `user_password_expires`       string      COMMENT 'Date when user\'s password expires\; null for no expiration date. Can also be set manually by calling User->expirePassword().'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:User_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/user'
;
