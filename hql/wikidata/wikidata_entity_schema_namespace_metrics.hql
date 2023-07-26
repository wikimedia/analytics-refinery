-- Generates metrics for the EntitySchema namespace on Wikidata
--
-- This script is used as input to a HiveToGraphite spark job.
-- The HiveToGraphite job will send the metrics generated to Graphite.
--
-- Parameters:
--     webrequest_table   -- The webrequest table to query.
--     year               -- year of metric to compute for.
--     month              -- month of metric to compute for.
--     day                -- day of metric to compute for.
--
-- Usage:
--     spark3-sql --master yarn -f wikidata_entity_schema_namespace_metrics.hql \
--         -d webrequest_table=wmf.webrequest                                   \
--         -d year=2022                                                         \
--         -d month=10                                                          \
--         -d day=11                                                            \
--
SELECT
    CONCAT(
        IF(namespace_id = 640, 'content', 'talk'),
        '.requests.',
        agent_type
    ) AS metric_id,
    COUNT(*),
    CAST('${year}-${month}-${day}' as timestamp ) as ts
FROM ${webrequest_table}
WHERE webrequest_source = 'text'
    AND year = ${year}
    AND month = ${month}
    AND day = ${day}
    AND uri_host = 'www.wikidata.org'
    AND is_pageview
    AND namespace_id IN (640, 641)
GROUP BY agent_type, namespace_id, year, month, day
