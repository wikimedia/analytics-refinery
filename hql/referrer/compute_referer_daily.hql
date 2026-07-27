-- Populate the referer_daily table
--
-- Usage:
-- spark3-sql -f compute_referer_daily.hql                                                    \
--            -d min_num_daily_referrals=500                                                  \
--            -d source_table=wmf_traffic.webrequest_v2                                       \
--            -d referer_daily_destination_table=wmf_traffic.referrer_daily                   \
--            -d countries_table=canonical_data.countries                                     \
--            -d coalesce_partitions=1                                                        \
--            -d year=2021                                                                    \
--            -d month=3                                                                      \
--            -d day=3
--

-- Delete existing data for the period to prevent duplication of data in case of recomputation
DELETE FROM ${referer_daily_destination_table}
WHERE day = TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0')), 'yyyy-MM-dd');

-- Compute data for the period
WITH se_pageviews AS (SELECT /*+ BROADCAST(countries) */
                             -- NOTE: geocoded_data['country'] can change unpredictably, use our canonical name instead
                             countries.name         AS country,
                             target.subdomain       AS lang,
                             client.browser_family  AS browser_family,
                             client.os_family       AS os_family,
                             request.referer_name   AS search_engine
                      FROM ${source_table}
                      LEFT OUTER JOIN ${countries_table} countries
                        ON countries.iso_code = geo.country_code
                      WHERE dt >= make_timestamp(${year}, ${month}, ${day}, 0, 0, 0)
                        AND dt < make_timestamp(${year}, ${month}, ${day}, 0, 0, 0) + INTERVAL '1' DAY
                        AND request_type = 'pageview'
                        AND agent_type = 'human'
                        AND request.referer_type = 'external (search engine)'
                        AND target.second_level_domain = 'wikipedia'
                        AND NOT COALESCE(countries.is_protected, FALSE)),
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
INSERT INTO ${referer_daily_destination_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
       country,
       lang,
       browser_family,
       os_family,
       search_engine,
       SUM(num_referrals) AS num_referrals,
       TO_DATE(CONCAT_WS('-', LPAD(${year}, 4, '0'), LPAD(${month}, 2, '0'), LPAD(${day}, 2, '0')), 'yyyy-MM-dd') AS day
FROM privacy_enforced
GROUP BY country,
         lang,
         search_engine,
         browser_family,
         os_family
HAVING SUM(num_referrals) >= ${min_num_daily_referrals}
ORDER BY day;
