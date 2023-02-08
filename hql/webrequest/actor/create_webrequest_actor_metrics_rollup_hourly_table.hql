-- Table holds metrics per webrequest actor agreggated hourly for the
-- last 24 hours period.
-- So if current partition is for hour=12 this table
-- agreggates data until hour 11 of day prior

-- Metrics are used to be able to identify
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
--     hive -f create_webrequest_actor_metrics_rollup_hourly_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `webrequest_actor_metrics_rollup_hourly`(
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
LOCATION '/wmf/data/wmf/webrequest_actor/metrics/rollup/hourly'
;
