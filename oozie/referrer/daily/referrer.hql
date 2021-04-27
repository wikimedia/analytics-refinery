-- Populate the referrer_daily table
--
-- Usage:
--     hive -f referrer.hql     \
--         -d refinery_hive_jar_path=hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar  \
--         -d min_num_daily_referrals=500                   \
--         -d source_table=wmf.pageview_actor               \
--         -d destination_table=milimetric.referrer_daily   \
--         -d year=2021                                     \
--         -d month=3                                       \
--         -d day=3                                         \
--

SET parquet.compression              = SNAPPY;
SET mapred.reduce.tasks              = 8;

ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION get_search_engine AS 'org.wikimedia.analytics.refinery.hive.GetRefererSearchEngineUDF';

WITH se_pageviews AS (
    SELECT
      geocoded_data['country'] AS country,
      normalized_host.project AS lang,
      user_agent_map['browser_family'] AS browser_family,
      user_agent_map['os_family'] AS os_family,
      get_search_engine(referer) AS search_engine
    FROM ${source_table}
    WHERE
      year = ${year}
      AND month = ${month}
      AND day = ${day}
      AND is_pageview
      AND agent_type = 'user'
      AND referer_class = 'external (search engine)'
      AND normalized_host.project_class = 'wikipedia'
      AND geocoded_data['country_code'] NOT IN ('DJ', 'GQ', 'ER', 'LA', 'KP', 'SO', 'TM')
),
pageview_counts AS (
    SELECT
      country,
      lang,
      browser_family,
      os_family,
      case
        when search_engine = 'Google Translate' then 'Google'
        when search_engine = 'Predicted Other' then 'other'
        else search_engine
      end AS search_engine,
      COUNT(1) AS num_referrals
    FROM se_pageviews
    GROUP BY
      country,
      lang,
      browser_family,
      os_family,
      search_engine
),
privacy_enforced AS (
    SELECT
      country,
      IF(num_referrals >= ${min_num_daily_referrals}, lang, 'other') AS lang,
      IF(num_referrals >= ${min_num_daily_referrals}, search_engine, 'other') AS search_engine,
      IF(num_referrals >= ${min_num_daily_referrals}, browser_family, 'other') AS browser_family,
      IF(num_referrals >= ${min_num_daily_referrals}, os_family, 'other') AS os_family,
      num_referrals
    FROM pageview_counts
)
INSERT OVERWRITE TABLE ${destination_table}
PARTITION(year=${year}, month=${month}, day=${day})
SELECT
  country,
  lang,
  browser_family,
  os_family,
  search_engine,
  SUM(num_referrals) AS num_referrals
FROM privacy_enforced
GROUP BY
  country,
  lang,
  search_engine,
  browser_family,
  os_family
HAVING
  SUM(num_referrals) >= ${min_num_daily_referrals}
;
