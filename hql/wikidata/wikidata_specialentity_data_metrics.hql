--Generates metrics for the wikidata Special:EntityData page
--
--This script is used as input to a HiveToGraphite spark job.
--The HiveToGraphite job will send the metrics generated to Graphite.
--
-- Parameters:
--     webrequest_table   -- The webrequest table to query.
--     year               -- year of metric to compute for.
--     month              -- month of metric to compute for.
--     day                -- day of metric to compute for.
--     coalesce_partitions-- number of partitions to reduce to.
--
-- Usage:
--     spark3-sql --master yarn -f wikidata_specialentity_data_metrics.hql \
--         -d webrequest_table=wmf.webrequest                              \
--         -d year=2021                                                    \
--         -d month=5                                                      \
--         -d day=4                                                        \
--         -d coalesce_partitions=4                                        \
--
CREATE TEMPORARY VIEW wd_specialentity_data AS
  SELECT
    agent_type,
    content_type,
    CASE WHEN content_type LIKE "%/rdf+xml%" THEN 'rdf'
      WHEN content_type LIKE "%/vnd.php%" THEN 'php'
      WHEN content_type LIKE '%/n-triples%' THEN 'nt'
      WHEN content_type LIKE '%/n3%' THEN 'n3'
      WHEN content_type LIKE '%/json%' THEN 'json'
      WHEN content_type LIKE '%/turtle%' THEN 'ttl'
      WHEN content_type LIKE '%/html%' THEN 'html'
      ELSE 'unknown'
    END AS format_key,
    user_agent,
    CAST('${year}-${month}-${day}' as timestamp ) as ts
  FROM ${webrequest_table}
  WHERE webrequest_source = 'text'
    AND year = ${year}
    AND month = ${month}
    AND day = ${day}
    AND http_status = 200
    AND normalized_host.project_class = 'wikidata'
    AND uri_path like '/wiki/Special:EntityData/%';

CACHE TABLE wd_specialentity_data;

WITH
agent_type_metric AS (
  SELECT /*+ COALESCE(${coalesce_partitions}) */
    CONCAT('agent_types.',agent_type),
    COUNT(1),
    ts
  FROM wd_specialentity_data
  GROUP BY
    agent_type,
    ts
),

format_content_metric AS (
  SELECT /*+ COALESCE(${coalesce_partitions}) */
    CONCAT('format.', format_key),
    COUNT(1),
    ts
  FROM wd_specialentity_data
  GROUP BY
    format_key,
    ts
),

wdqs_updater_agent_type_metric AS (
  SELECT /*+ COALESCE(${coalesce_partitions}) */
    CONCAT('wdqs_updater.agent_types.',agent_type),
    COUNT(1),
    ts
  FROM wd_specialentity_data
  WHERE user_agent LIKE 'Wikidata Query Service Updater%'
  GROUP BY
    agent_type,
    ts
),

wdqs_updater_format_content_metric AS (
  SELECT /*+ COALESCE(${coalesce_partitions}) */
    CONCAT('wdqs_updater.format.', format_key),
    COUNT(1),
    ts
  FROM wd_specialentity_data
  WHERE user_agent LIKE 'Wikidata Query Service Updater%'
  GROUP BY
    format_key,
    ts
)

SELECT * FROM agent_type_metric
UNION
SELECT * FROM format_content_metric
UNION
SELECT * FROM wdqs_updater_agent_type_metric
UNION
SELECT * FROM wdqs_updater_format_content_metric
