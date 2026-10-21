--
-- Computes hourly User-Agent compliance metrics from webrequest.
-- per https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
--
-- Parameters:
--     source_table              Fully qualified webrequest source table.
--     user_agents_info_table    Fully qualified User-Agents info table.
--     destination_table         Fully qualified destination table.
--     year                      Year of the partition to process.
--     month                     Month of the partition to process.
--     day                       Day of the partition to process.
--     hour                      Hour of the partition to process.
--
-- Usage:
--     spark3-sql -f compute_user_agent_compliance_hourly.hql \
--         -d source_table=wmf.webrequest \
--         -d user_agents_info_table=wmf_traffic.user_agents_info \
--         -d destination_table=wmf_traffic.user_agent_compliance_hourly \
--         -d year=2026
--         -d month=2
--         -d day=1
--         -d hour=0

DELETE FROM ${destination_table}
WHERE hour = MAKE_TIMESTAMP(${year}, ${month}, ${day}, ${hour}, 0, 0)
;

WITH requests AS (
    SELECT
        wr.webrequest_source,
        CAST(wr.http_status AS INT) AS http_status,
        CASE
            WHEN wr.cache_status IN ('hit-front', 'hit-local') THEN 'hit'
            WHEN wr.cache_status IN ('int-front', 'int-local', 'int-tls') THEN 'int'
            WHEN wr.cache_status = 'pass' THEN 'pass'
            WHEN wr.cache_status = 'miss' THEN 'miss'
            ELSE 'unknown'
        END AS cache_status,
        wr.is_api_request,
        wr.uri_path RLIKE '^/w/index\\.php' AS is_endpoint_index_php,
        wr.uri_path LIKE '/w/load.php%' AS is_endpoint_load_php,
        CAST(ua.identified_agent = 'instant_commons' AS BOOLEAN) AS is_instant_commons_agent,
        CAST(ua.identified_agent = 'mw_foreign_repo' AS BOOLEAN) AS is_foreign_api_agent,
        CAST(ua.identified_agent = 'wikipedia_app' AS BOOLEAN) AS is_wikipedia_app_agent,
        wr.ip_provenance,
        COALESCE(ua.ua_policy_compliance_level, 'not_applicable') AS compliance_status,
        SORT_ARRAY(MAP_KEYS(ua.contact_info)) AS contact_method,
        wr.user_agent,
        wr.response_size,
        wr.time_firstbyte
    FROM ${source_table} wr
    LEFT JOIN ${user_agents_info_table} ua
           ON wr.user_agent = ua.user_agent
          AND ua.hour = MAKE_TIMESTAMP(${year}, ${month}, ${day}, ${hour}, 0, 0)
          -- the table has one row per User-Agent per source; filter to avoid duplicate counts
          AND ua.source = 'webrequest'
    WHERE wr.year  = ${year}
      AND wr.month = ${month}
      AND wr.day   = ${day}
      AND wr.hour  = ${hour}
      AND wr.webrequest_source IN ('text', 'upload')
)
INSERT INTO ${destination_table}
SELECT /*+ COALESCE(1) */
    webrequest_source,
    http_status,
    cache_status,
    is_api_request,
    is_endpoint_index_php,
    is_endpoint_load_php,
    is_instant_commons_agent,
    is_foreign_api_agent,
    is_wikipedia_app_agent,
    ip_provenance,
    compliance_status,
    contact_method,
    COUNT(*) AS request_count,
    SUM(response_size) AS response_size_total,
    SUM(time_firstbyte) AS ttfb_sec_total,
    COUNT(DISTINCT user_agent) AS unique_user_agent_count,
    MAKE_TIMESTAMP(${year}, ${month}, ${day}, ${hour}, 0, 0) AS hour
FROM requests
GROUP BY
    webrequest_source,
    http_status,
    cache_status,
    is_api_request,
    is_endpoint_index_php,
    is_endpoint_load_php,
    is_instant_commons_agent,
    is_foreign_api_agent,
    is_wikipedia_app_agent,
    ip_provenance,
    compliance_status,
    contact_method
;
