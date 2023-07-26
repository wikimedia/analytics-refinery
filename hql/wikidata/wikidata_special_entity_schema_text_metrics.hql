-- Generates metrics for the wikidata Special:EntitySchemaText page
--
--This script is used as input to a HiveToGraphite spark job.
--The HiveToGraphite job will send the metrics generated to Graphite.
--
-- Parameters:
--     webrequest_table   -- The webrequest table to query.
--     year               -- year of metric to compute for.
--     month              -- month of metric to compute for.
--     day                -- day of metric to compute for.
--
-- Usage:
--     spark3-sql --master yarn -f wikidata_special_entity_schema_text_metrics.hql \
--         -d webrequest_table=wmf.webrequest                                      \
--         -d year=2021                                                            \
--         -d month=5                                                              \
--         -d day=4                                                                \

SELECT
    'requests' as metric_id,
    COUNT(*),
    CAST('${year}-${month}-${day}' as timestamp ) as ts
FROM ${webrequest_table}
WHERE webrequest_source = 'text'
    AND year = ${year}
    AND month = ${month}
    AND day = ${day}
    AND uri_host = 'www.wikidata.org'
    AND uri_path like '/wiki/Special:EntitySchemaText/%'
GROUP BY year, month, day
