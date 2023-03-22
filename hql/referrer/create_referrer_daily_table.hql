-- Creates table statement for daily aggregated referrer table.
--
-- NOTE:  When choosing partition field types,
-- one should take into consideration Hive's
-- insistence on storing partition values
-- as strings.  See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
-- and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_referrer_daily_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `referrer_daily` (
    `country`             string  COMMENT 'Reader country per IP geolocation',
    `lang`                string  COMMENT 'Wikipedia language -- e.g., en for English',
    `browser_family`      string  COMMENT 'Browser family from user-agent',
    `os_family`           string  COMMENT 'OS family from user-agent',
    `search_engine`       string  COMMENT 'One of ~20 standard search engines (e.g., Google)',
    `num_referrals`       int     COMMENT 'Number of pageviews from the referral source'
)
PARTITIONED BY (
    `year`                int     COMMENT 'Unpadded year of request',
    `month`               int     COMMENT 'Unpadded month of request',
    `day`                 int     COMMENT 'Unpadded day of request'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/referrer/daily'
;
