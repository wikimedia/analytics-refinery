-- Table holds label per actor.

-- Label can be "automated" if we estimate actor is a bot or "user" otherwise
-- Labels are assigned hourly using features calculated with data from the 24 hours prior

-- TODO once this is is been running for a while we need to see whether there are actors
-- whose label shifts

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
--     hive -f create_actor_label_hourly.hql \
--         --database predictions
--

CREATE EXTERNAL TABLE IF NOT EXISTS `actor_label_hourly`(
    `version`                     string  COMMENT 'Version of the classifier that provided the label',
    `actor_signature`             string  COMMENT 'Actor signature',
    `label`                       string  COMMENT 'Label assigned by heuristics or ML classifier, so far we have two: <user> or <automated>',
    `label_reason`                string  COMMENT 'Explains the main reason for assignation of label'
)
PARTITIONED BY (
    `year`                int    COMMENT 'Unpadded year',
    `month`               int    COMMENT 'Unpadded month',
    `day`                 int    COMMENT 'Unpadded day',
    `hour`                int    COMMENT 'Unpadded hour'
)
STORED AS PARQUET
LOCATION '/wmf/data/learning/predictions/actor/hourly'
;
