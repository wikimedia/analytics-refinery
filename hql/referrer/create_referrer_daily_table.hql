-- Creates table statement for daily aggregated referrer table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_referrer_daily_table.hql    \
--     --database wmf_traffic                           \
--     -d location=/wmf/data/wmf_traffic/referrer/daily
--

CREATE EXTERNAL TABLE IF NOT EXISTS `referrer_daily`(
    `country`             string  COMMENT 'Reader country per IP geolocation',
    `lang`                string  COMMENT 'Wikipedia language -- e.g., en for English',
    `browser_family`      string  COMMENT 'Browser family from user-agent',
    `os_family`           string  COMMENT 'OS family from user-agent',
    `search_engine`       string  COMMENT 'One of ~20 standard search engines (e.g., Google)',
    `num_referrals`       int     COMMENT 'Number of pageviews from the referral source',
    `day`                 date    COMMENT 'The date of the request'
)
USING ICEBERG
PARTITIONED BY (months(day))
LOCATION '${location}'
;
