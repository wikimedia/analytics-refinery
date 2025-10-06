-- Creates table statement for raw centralauth_globaluser table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql \
--       --database wmf_raw \
--       -f create_centralauth_globaluser_table.hql \
--

CREATE EXTERNAL TABLE `centralauth_globaluser`(
  gu_id bigint COMMENT 'Internal unique ID for the authentication server', 
  gu_name string COMMENT 'The user name.',
  gu_home_db string COMMENT 'Local database name of the user\'s home wiki. By default, the winner of a migration check for old accounts or the account the user was first registered at for new ones. May be changed over time. Note that this field is not always set (T316472) and may not be reliable.',
  gu_email string COMMENT 'Registered email address.  Redacted and always NULL in Data Lake table.',
  gu_email_authenticated string COMMENT 'Timestamp when the address was confirmed as belonging to the user. NULL if not confirmed.',
  gu_password string COMMENT 'hashed password. Redacted and always NULL in Data Lake table.',
  gu_locked boolean COMMENT 'If true, this account cannot be used to log in on any wiki.',
  gu_hidden_level int COMMENT 'If true, this account should be hidden from most public user lists. Used for deleting accounts without breaking referential integrity.',
  gu_registration string COMMENT 'Registration time',
  gu_password_reset_key string COMMENT 'Random key for password resets. Redacted and always NULL in Data Lake table.',
  gu_password_reset_expiration string,
  gu_auth_token string COMMENT 'Random key for crosswiki authentication tokens. Redacted and always NULL in Data Lake table.',
  gu_cas_token string COMMENT 'Value used for CAS operations. Redacted and always NULL in Data Lake table.'
)
COMMENT
  'Global account data. See most up to date documentation at https://www.mediawiki.org/wiki/Extension:CentralAuth/globaluser_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki_private/tables/centralauth_globaluser'
;
