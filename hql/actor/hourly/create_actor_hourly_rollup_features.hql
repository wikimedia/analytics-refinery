-- Table holds features per actor agreggated for the last 24 hours period.
-- So if current partition is for hour=12 this table
-- agreggates data until hour 11 of day prior

-- Features are used to be able to identify
-- high volume bot spikes for traffic not self identified
-- as bots
--

-- NOTE:  When choosing partition field types,
-- one should take into consideration Hive's
-- insistence on storing partition values
-- as strings. See:
-- https://wikitech.wikimedia.org/wiki/File:Hive_partition_formats.png
-- and
-- http://bots.wmflabs.org/~wm-bot/logs/%23wikimedia-analytics/20140721.txt
--
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_actor_hourly_rollup_features.hql \
--         --database features
--

CREATE EXTERNAL TABLE IF NOT EXISTS `actor_rollup_hourly`(
  `version`                           string  COMMENT 'Version of the algorithm that calculates the feature rollup',
  `actor_signature`                   string  COMMENT 'Actor signature',
  `pageview_count`                    bigint  COMMENT 'Number of pageviews',
  `pageview_rate_per_min`             int     COMMENT 'Number of pageviews per minute',
  `nocookies`                         bigint  COMMENT 'Sum() of all nocookies values in X-Analytics for all pageviews',
  `user_agent_length`                 int     COMMENT 'Length of user-agent field',
  `avg_distinct_pages_visited_count`  double  COMMENT 'Average number of distinct pages visited per hour',
  `rolled_up_hours`                   int     COMMENT 'Number of hours for which data was present to compute the rollup'
)
PARTITIONED BY (
    `year`                int    COMMENT 'Unpadded year',
    `month`               int    COMMENT 'Unpadded month',
    `day`                 int    COMMENT 'Unpadded day',
    `hour`                int    COMMENT 'Unpadded hour'
)
STORED AS PARQUET
LOCATION '/wmf/data/learning/features/actor/rollup/hourly'
;
