-- Populate the referer_daily table
--
-- Usage:
-- spark3-sql -f compute_referer_daily.hql                                                    \
--            -d min_num_daily_referrals=500                                                  \
--            -d source_table=wmf.pageview_actor                                              \
--            -d referer_daily_destination_table=wmf.referer_daily                           \
--            -d coalesce_partitions=4                                                        \
--            -d year=2021                                                                    \
--            -d month=3                                                                      \
--            -d day=3                                                                        \
--


WITH se_pageviews AS (SELECT geocoded_data['country']         AS country,
                             normalized_host.project          AS lang,
                             user_agent_map['browser_family'] AS browser_family,
                             user_agent_map['os_family']      AS os_family,
                             referer_data.referer_name        AS search_engine
                      FROM ${source_table}
                      WHERE year = ${year}
                        AND month = ${month}
                        AND day = ${day}
                        AND is_pageview
                        AND agent_type = 'user'
                        AND referer_data.referer_class = 'external (search engine)'
                        AND normalized_host.project_class = 'wikipedia'
                        AND geocoded_data['country_code'] NOT IN ('DJ', 'GQ', 'ER', 'LA', 'KP', 'SO', 'TM')),
     pageview_counts AS (SELECT country,
                                lang,
                                browser_family,
                                os_family,
                                CASE
                                    WHEN search_engine = 'Google Translate' then 'Google'
                                    WHEN search_engine = 'Predicted Other' then 'other'
                                    ELSE search_engine
                                    END  AS search_engine,
                                COUNT(1) AS num_referrals
                         FROM se_pageviews
                         GROUP BY country,
                                  lang,
                                  browser_family,
                                  os_family,
                                  search_engine),
     privacy_enforced AS (SELECT country,
                                 IF(num_referrals >= ${min_num_daily_referrals}, lang, 'other')           AS lang,
                                 IF(num_referrals >= ${min_num_daily_referrals}, search_engine, 'other')  AS search_engine,
                                 IF(num_referrals >= ${min_num_daily_referrals}, browser_family,
                                    'other')                                                              AS browser_family,
                                 IF(num_referrals >= ${min_num_daily_referrals}, os_family, 'other')      AS os_family,
                                 num_referrals
                          FROM pageview_counts)
INSERT OVERWRITE TABLE ${referer_daily_destination_table}
PARTITION (year = ${year}, month = ${month}, day = ${day})
SELECT /*+ COALESCE(${coalesce_partitions}) */
       country,
       lang,
       browser_family,
       os_family,
       search_engine,
       SUM(num_referrals) AS num_referrals
FROM privacy_enforced
GROUP BY country,
         lang,
         search_engine,
         browser_family,
         os_family
HAVING SUM(num_referrals) >= ${min_num_daily_referrals};
