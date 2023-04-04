-- Create table statement for disallowed_cassandra_articles table.
--
-- Update procedure:
--   * Fetch the disallowed list
--     ssh an-launcher1002.eqiad.wmnet
--     export TSV_FILENAME=disallowed_cassandra_articles.tsv
--     export TSV_HDFS_PATH="/wmf/refinery/current/static_data/cassandra/${TSV_FILENAME}"
--     hdfs dfs -cat $TSV_HDFS_PATH > $TSV_FILENAME
--   * Add or remove some entries (beware, tabs are expected between columns, not spaces)
--     vim $TSV_FILENAME
--   * Push the file back to HDFS
--     sudo -u hdfs kerberos-run-command hdfs hdfs dfs -put -f $TSV_FILENAME $TSV_HDFS_PATH
--     sudo -u hdfs kerberos-run-command hdfs hdfs dfs -chmod +r $TSV_HDFS_PATH
--
-- Parameters:
--     database Should be `wmf`
--
-- Usage
--     spark3-sql \
--     -f create_disallowed_cassandra_articles_table.hql   \
--     --database user
--

CREATE EXTERNAL TABLE `disallowed_cassandra_articles` (
    `project` string COMMENT 'Name of the wiki project',
    `article` string COMMENT 'Title of the disallowed article (case insensitive comparison)'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
-- Set table location relative to the current refinery folder
LOCATION '/wmf/refinery/current/static_data/cassandra/disallowed_cassandra_articles';
