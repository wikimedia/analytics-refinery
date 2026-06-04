--
-- Computes hourly User-Agent compliance metrics from webrequest.
-- per https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
--
-- Parameters:
--     source_table              Fully qualified webrequest source table.
--     ua_classification_table   Fully qualified UA classification table.
--     destination_table         Fully qualified destination table.
--     year                      Year of the partition to process.
--     month                     Month of the partition to process.
--     day                       Day of the partition to process.
--     hour                      Hour of the partition to process.
--
-- Usage:
--     spark3-sql -f compute_user_agent_compliance_hourly.hql \
--         -d source_table=wmf.webrequest \
--         -d ua_classification_table=wmf_traffic.user_agent_compliance_classified_hourly \
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
        ua.is_instant_commons_agent,
        ua.is_foreign_api_agent,
        ua.is_wikipedia_app_agent,
        wr.ip_provenance,
        COALESCE(ua.compliance_level, 'not_applicable') AS compliance_status,
        ua.contact_method,
        wr.user_agent,
        wr.response_size,
        wr.time_firstbyte
    FROM ${source_table} wr
    LEFT JOIN ${ua_classification_table} ua
           ON wr.user_agent = ua.user_agent
          AND ua.hour = MAKE_TIMESTAMP(${year}, ${month}, ${day}, ${hour}, 0, 0)
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
