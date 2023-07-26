--Generates monthly metric for Wikidata CoEditors.
--
--The hql script is used as input to a HiveToGraphite spark job which is scheduled on Airflow
--The output of this script would be sent to Graphite.
--
-- Parameters:
--     mw_project_namespace_map_table -- The namespace map table to query.
--     mw_history_table               -- The history table to query.
--     snapshot                       -- month YYYY-MM of snapshot to compute metric for.
--
-- Usage:
--     spark3-sql --master yarn -f coeditors_metrics.hql                              \
--         -d mw_project_namespace_map_table=wmf_raw.mediawiki_project_namespace_map  \
--         -d mw_history_table=wmf.mediawiki_history                                  \
--         -d snapshot=2022-02                                                        \
--

WITH
wikipedias AS (
  SELECT DISTINCT
    dbname
  FROM ${mw_project_namespace_map_table}
  WHERE snapshot = '${snapshot}'
    AND hostname LIKE '%wikipedia.org'
),

filtered_history AS (
  SELECT wiki_db,
    event_user_text
  FROM ${mw_history_table}
  WHERE snapshot = '${snapshot}'
    AND event_entity = 'revision'
    AND event_type = 'create'
    AND NOT revision_is_deleted_by_page_deletion
    AND NOT event_user_is_anonymous
    AND NOT ARRAY_CONTAINS(event_user_groups, 'bot')
    AND event_timestamp LIKE '${snapshot}%'
),

wikidata_editors AS (
  SELECT DISTINCT
    event_user_text
  FROM filtered_history
  WHERE wiki_db = 'wikidatawiki'
)

SELECT
  fh.wiki_db,
  COUNT(DISTINCT fh.event_user_text) as wikidata_coeditors,
  cast(concat('${snapshot}', '-01') as timestamp) as event_time
FROM filtered_history fh
  JOIN wikipedias w ON (fh.wiki_db = w.dbname)
  JOIN wikidata_editors wde ON (fh.event_user_text = wde.event_user_text)
GROUP BY
  fh.wiki_db
ORDER BY wikidata_coeditors DESC
LIMIT 1200;
