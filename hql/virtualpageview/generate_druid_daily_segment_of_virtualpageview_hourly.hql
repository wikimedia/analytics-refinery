-- Loads one day of hourly virtualpageviews into a temp table so it can be loaded
--   into Druid.  This is loading daily segments with more dimensions for the
--   latest month.  A companion monthly job overwrites the daily segments with
--   monthly segments, with fewer dimensions, for longer retention.
--
-- Usage
--     spark3-sql --master yarn \
--         -f generate_druid_daily_segment_of_virtualpageview_hourly.hql   \
--         -d source_table=wmf.virtualpageview_hourly                      \
--         -d destination_table=milimetric.tmp_daily_virtualpageviews_test \
--         -d destination_directory=/tmp/druid/daily_virtualpageviews_test \
--         -d year=2023                                                    \
--         -d month=4                                                      \
--         -d day=26
--

DROP TABLE IF EXISTS ${destination_table};

CREATE TABLE IF NOT EXISTS ${destination_table} (
    `dt`                  string,
    `project`             string,
    `language_variant`    string,
    `access_method`       string,
    `agent_type`          string,
    `referer_class`       string,
    `continent`           string,
    `country_code`        string,
    `country`             string,
    `subdivision`         string,
    `city`                string,
    `ua_device_family`    string,
    `ua_browser_family`   string,
    `ua_browser_major`    string,
    `ua_os_family`        string,
    `ua_os_major`         string,
    `ua_os_minor`         string,
    `ua_wmf_app_version`  string,
    `view_count`          bigint
)
USING PARQUET
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';

INSERT OVERWRITE TABLE ${destination_table}
SELECT /*+ coalesce(4) */
    CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), 'T',
        LPAD(hour, 2, '0'), ':00:00Z') AS dt,
    project AS project,
    language_variant AS language_variant,
    access_method AS access_method,
    agent_type AS agent_type,
    referer_class AS referer_class,
    continent AS continent,
    country_code AS country_code,
    country AS country,
    subdivision AS subdivision,
    city AS city,
    user_agent_map['device_family'] AS ua_device_family,
    user_agent_map['browser_family'] AS ua_browser_family,
    user_agent_map['browser_major'] AS ua_browser_major,
    user_agent_map['os_family'] AS ua_os_family,
    user_agent_map['os_major'] AS ua_os_major,
    user_agent_map['os_minor'] AS ua_os_minor,
    user_agent_map['wmf_app_version'] AS ua_wmf_app_version,
    SUM(view_count) AS view_count
FROM ${source_table}
WHERE year = ${year}
    AND month = ${month}
    AND day = ${day}
GROUP BY
    CONCAT(
        LPAD(year, 4, '0'), '-',
        LPAD(month, 2, '0'), '-',
        LPAD(day, 2, '0'), 'T',
        LPAD(hour, 2, '0'), ':00:00Z'),
    project,
    language_variant,
    access_method,
    agent_type,
    referer_class,
    continent,
    country_code,
    country,
    subdivision,
    city,
    user_agent_map['device_family'],
    user_agent_map['browser_family'],
    user_agent_map['browser_major'],
    user_agent_map['os_family'],
    user_agent_map['os_major'],
    user_agent_map['os_minor'],
    user_agent_map['wmf_app_version']
;
