-- Create table statement for disallowed_cassandra_articles table.
--
-- This table is used to filter out some wiki articles from aggregation stats following data manipulation attacks.
--
-- More details here:
-- https://wikitech.wikimedia.org/wiki/Data_Engineering/Systems/AQS#Data_filter_before_Cassandra_load
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
