--
-- Computes the top N non-compliant User-Agents by request volume across various dimensions.
-- per https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
--
-- Parameters:
--     source_table              Fully qualified webrequest source table.
--     ua_classification_table   Fully qualified UA classification table.
--     destination_table         Fully qualified destination table.
--     year                      Year of the partition to process.
--     month                     Month of the partition to process.
--     day                       Day of the partition to process.
--     top_n                     Maximum rank to retain per partition group (e.g. 50).
--
-- Usage:
--     spark3-sql -f compute_user_agent_top_non_compliant_daily.hql \
--         -d source_table=wmf.webrequest \
--         -d ua_classification_table=wmf_traffic.user_agent_compliance_classified_hourly \
--         -d destination_table=wmf_traffic.user_agent_top_non_compliant_daily \
--         -d year=2026 \
--         -d month=2 \
--         -d day=1 \
--         -d top_n=50

DELETE FROM ${destination_table}
WHERE day = MAKE_DATE(${year}, ${month}, ${day})
;

WITH base AS (
    SELECT
        wr.webrequest_source,
        ua.compliance_level AS compliance_status,
        wr.ip_provenance,
        ua.is_instant_commons_agent,
        ua.is_foreign_api_agent,
        ua.is_wikipedia_app_agent,
        wr.is_api_request,
        CAST(wr.http_status AS INT) AS http_status,
        wr.user_agent,
        MAKE_DATE(${year}, ${month}, ${day}) AS day
    FROM ${source_table} wr
    INNER JOIN ${ua_classification_table} ua
           ON wr.user_agent = ua.user_agent
          AND CAST(ua.hour AS DATE) = MAKE_DATE(${year}, ${month}, ${day})
          AND ua.compliance_level IN ('non_compliant', 'non_compliant_placeholder')
    WHERE wr.year  = ${year}
      AND wr.month = ${month}
      AND wr.day   = ${day}
      AND wr.webrequest_source IN ('text', 'upload')
),
aggregated AS (
    SELECT
        webrequest_source,
        compliance_status,
        ip_provenance,
        is_instant_commons_agent,
        is_foreign_api_agent,
        is_wikipedia_app_agent,
        is_api_request,
        http_status,
        user_agent,
        day,
        COUNT(*) AS request_count
    FROM base
    GROUP BY
        webrequest_source,
        compliance_status,
        ip_provenance,
        is_instant_commons_agent,
        is_foreign_api_agent,
        is_wikipedia_app_agent,
        is_api_request,
        http_status,
        user_agent,
        day
),
ranked AS (
    SELECT
        *,
        RANK() OVER (
            PARTITION BY
                webrequest_source,
                compliance_status,
                ip_provenance,
                is_instant_commons_agent,
                is_foreign_api_agent,
                is_wikipedia_app_agent,
                is_api_request,
                http_status
            ORDER BY request_count DESC
        ) AS rank
    FROM aggregated
)
INSERT INTO ${destination_table}
SELECT /*+ COALESCE(1) */
    webrequest_source,
    compliance_status,
    ip_provenance,
    is_instant_commons_agent,
    is_foreign_api_agent,
    is_wikipedia_app_agent,
    is_api_request,
    http_status,
    rank,
    user_agent,
    request_count,
    day
FROM ranked
WHERE rank <= ${top_n}
;
