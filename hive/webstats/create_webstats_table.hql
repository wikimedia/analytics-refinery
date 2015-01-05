-- Creates table for hourly webstats output
--
-- NOTE:  When choosing partition field types,
-- one should take into consideration Hive's
-- insistence on storing partition values
-- as strings.  See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
-- and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
--
-- Since processing the hourly partitions for this table is not much
-- affected by TEXTFILE vs. SEQUENCEFILE, we store as TEXTFILE, since
-- they are smaller than SEQUENCEFILEs.
--
--   +--------------+---------------------------+--------------+
--   | STORED AS    | hive.exec.compress.output | Size in HDFS |
--   +--------------+---------------------------+--------------+
--   | TEXTFILE     | true                      | 264MB        |
--   | SEQUENCEFILE | true                      | 297MB        |
--   | TEXTFILE     | false                     | 665MB        |
--   | SEQUENCEFILE | false                     | 821MB        |
--   +--------------+---------------------------+--------------+
--                                     (For 2014-09-15T12:xx:xx)
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_webstats_table.hql \
--         --database wmf
--

CREATE TABLE IF NOT EXISTS `webstats` (
    `qualifier`           string COMMENT 'Language/site/project identifier',
    `page_title`          string COMMENT 'Title of the article',
    `count_views`         bigint COMMENT 'Summed up pageviews',
    `total_response_size` bigint COMMENT 'Summed up response sizes')
PARTITIONED BY (
    `year`                int    COMMENT 'Unpadded year of request',
    `month`               int    COMMENT 'Unpadded month of request',
    `day`                 int    COMMENT 'Unpadded day of request',
    `hour`                int    COMMENT 'Unpadded hour of request')
STORED AS TEXTFILE
LOCATION '/wmf/data/wmf/webstats'
;
