-- Table holds metrics per webrequest actor computed hourly.
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
--     hive -f create_webrequest_actor_metrics_hourly_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `webrequest_actor_metrics_hourly`(
  `version`                      string  COMMENT 'Version of software that calculates these features',
  `actor_signature`              string  COMMENT 'Actor signature as a hash',
  `first_interaction_dt`         string  COMMENT 'UTC timestamp in ISO 8601 format (e.g. 2019-08-27T14:00:00Z) of the actor first interaction for that hour',
  `last_interaction_dt`          string  COMMENT 'UTC timestamp in ISO 8601 format (e.g. 2019-08-27T14:00:00Z) of the actor last interaction for that hour',
  `pageview_count`               bigint  COMMENT 'Number of pageviews',
  `pageview_rate_per_min`        int     COMMENT 'Number of pageviews per minute',
  `nocookies`                    bigint  COMMENT 'Sum() of all nocookies values in X-Analytics for all pageviews',
  `user_agent_length`            int     COMMENT 'Length of user-agent field',
  `distinct_pages_visited_count` bigint  COMMENT 'Number of distinct visited pages'
)
PARTITIONED BY (
    `year`                int    COMMENT 'Unpadded year',
    `month`               int    COMMENT 'Unpadded month',
    `day`                 int    COMMENT 'Unpadded day',
    `hour`                int    COMMENT 'Unpadded hour'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/webrequest_actor/metrics/hourly'
;
