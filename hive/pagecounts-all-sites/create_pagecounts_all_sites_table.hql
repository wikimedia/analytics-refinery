-- Creates table for hourly pagecounts-all-sites output
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
-- Actually, the table should be external. But when it got created,
-- hive.insert.into.external.tables was still set to false. Hence, we have been
-- forced to having it internal back then, as otherwise we could not insert into
-- that table from within Hive. Now that Hive allows to insert into external
-- tables, this table can be turned into an external table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_pagecounts_all_sites_table.hql \
--         --database wmf
--

CREATE TABLE IF NOT EXISTS `pagecounts_all_sites` (
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
LOCATION '/wmf/data/wmf/pagecounts-all-sites'
;
