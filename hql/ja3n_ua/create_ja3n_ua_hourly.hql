--
-- Creates a Hive ja3n_ua_hourly table.
--
-- This table uses Hill numbers of order 2, or Neff (for short):
-- 1 / SUM(Pi^2) where Pi is the probability P of a category i in a population.
-- The number indicates how many of the population's categories are effective.
-- Meaning how many of them carry the bulk of the population.
--
-- The Neff is calculated for 2 populations:
--     - JA3Ns over a given User-Agent: ja3n_neff
--     - User-Agents over a given JA3N: ua_neff
--
-- These Neffs are used to divide the category ranks, thus normalizing them:
--     - Normalized rank of JA3Ns over a given User-Agent: ja3n_norm_rank
--     - Normalized rank of User-Agents over a given JA3N: ua_norm_rank
-- If the normalized rank is between 0 and 1 (inclusive) the category is effective.
-- The more the normalized rank grows greater than 1, the more uncommon the category is.
--
-- Parameters:
--     table_name  Name of the table.
--     location    Location of the table.
--
-- Usage:
--     spark3-sql -f create_ja3n_ua_hourly.hql \
--         --database wmf_traffic \
--         -d table_name=ja3n_ua_hourly \
--         -d location=/wmf/data/wmf_traffic/ja3n_ua/hourly
--


CREATE EXTERNAL TABLE IF NOT EXISTS `${table_name}` (
    `ja3n`            STRING               COMMENT  "JA3N fingerprint.",
    `user_agent`      STRING               COMMENT  "User-Agent string.",
    `user_agent_map`  MAP<STRING, STRING>  COMMENT  "Parsed User-Agent map.",
    `request_count`   BIGINT               COMMENT  "Request count for this ja3n+user_agent pair. It can be aggregated across ja3n, user_agent and dt.",
    `ja3n_norm_rank`  FLOAT                COMMENT  "Normalized rank of this ja3n, given its user_agent (see table description above).",
    `ua_norm_rank`    FLOAT                COMMENT  "Normalized rank of this user_agent, given its ja3n (see table description above)."
)
PARTITIONED BY (
    `year`            INT                  COMMENT  "Year of the aggregated requests.",
    `month`           INT                  COMMENT  "Month of the aggregated requests (unpadded).",
    `day`             INT                  COMMENT  "Day of the aggregated requests (unpadded).",
    `hour`            INT                  COMMENT  "Hour of the aggregated requests (unpadded)."
)
STORED AS PARQUET
LOCATION "${location}"
;
