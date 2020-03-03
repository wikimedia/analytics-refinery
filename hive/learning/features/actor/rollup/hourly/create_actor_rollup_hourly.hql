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
--     hive -f create_actor_rollup_hourly.hql \
--         --database features
--

CREATE EXTERNAL TABLE IF NOT EXISTS `actor_rollup_hourly`(
  `version`                     string  COMMENT 'Version of the algorithm that calculates the feature rollup',
  `actor_id`                    string  COMMENT 'Actor Identifier',
  `interaction_start_ts`        string  COMMENT 'UTC timestamp in ISO 8601 format (e.g. 2019-08-27T14:00:00Z)',
  `interaction_end_ts`          string  COMMENT 'UTC timestamp in ISO 8601 format (e.g. 2019-08-27T14:00:00Z)',
  `interaction_length_secs`     bigint  COMMENT 'Interaction length in seconds so as not to have to calculate it with start and end intervals',
  `pageview_count`              bigint  COMMENT 'Number of pageviews',
  `pageview_ratio_per_min`      int     COMMENT 'Pageviews per minute',
  `nocookies`                   bigint  COMMENT 'Sum() of all nocookies values in X-Analytics for all pageviews',
  `user_agent_length`           int     COMMENT 'Length of user-agent field'
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
