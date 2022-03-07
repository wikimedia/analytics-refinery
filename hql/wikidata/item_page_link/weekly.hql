-- Generate a wikidata_item_page_link snapshot from a wikidata entity table.
-- Data is saved in parquet-format in a partition-folder.
--
-- This script is executed by Spark, and scheduled on Airflow.
--
-- A page-item links is a link between a wikidata item and its related wikipedia
-- pages in various languages.
--
-- Steps:
-- 1- snapshot_page_titles: Loads a list of pages with their id, db and namesapce.
--    from the snapshot.
-- 2- event_page_titles: Loads the last data about pages from the events.
-- 3- current_page_titles: Updates snapshot_page_titles with the events data.
-- 4- localized_namespace_page_titles: Translates the page namespaces.
-- 5- wikidata_sitelinks: Loads the links form the entity table.
-- 6- Provides the page information for each link.
-- 7- Unloads the data to a partition.
--
-- Parameters:
-- * destination_table: Fully qualified hive table name.
-- * history_snapshot: Snapshot month of the tables to query.
--   Usually in format YYYY-MM
-- * wikidata_snapshot: Snapshot day of the wikidata-entity table to query.
--   Usually in format YYYY-MM-DD
-- * coalesce_partitions: The number of files as a result.
--
-- Usage:
-- hive -f hdfs://wmf/refinery/current/hql/wikidata/item_page_link/weekly.hql                 \
--     -d destination_table=wmf.wikidata_item_page_link
--     -d wikidata-snapshot=2020-01-13
--     -d history-snapshot=2020-01
--
-- Note: The COALESCE hint was added in Spark 2.4. Here, it is used to set the
--       maximum number of output files.
--
-- Important limitation: 
-- Below, we join to the eventPageMoveTable to get page titles since
-- the last history snapshot, because these change quite rapidly.  This
-- improves the ability to link to wikidata items, but it does not make it
-- perfect.
-- One problem that remains is pages that have been deleted since
-- the history snapshot will remain and effectively duplicate links to wikidata
-- items.  This could be remedied by joining to mediawiki_page_delete but then
-- we'd have to also join to page_restore and it gets complicated.  Instead,
-- we propose to wait until we have a more comprehensive incremental update
-- of mediawiki history.

set spark.sql.parquet.compression.codec = snappy;

WITH

snapshot_page_titles AS (
  SELECT DISTINCT
    wiki_db,
    page_id,
    first_value(page_title) OVER w AS page_title,
    first_value(page_namespace) OVER w AS page_namespace
  FROM wmf.mediawiki_page_history
  WHERE snapshot = '${history_snapshot}'
    AND page_id IS NOT NULL AND page_id > 0
    AND page_title IS NOT NULL and LENGTH(page_title) > 0
  WINDOW w AS (
    PARTITION BY
      wiki_db,
      page_id
    ORDER BY
      start_timestamp DESC, -- If events have the same timestamp
      source_log_id DESC,   -- Use biggest source_log_id. If same source_log_id
      caused_by_event_type  -- then use create instead of delete.
  )
),

event_page_titles AS (
  SELECT DISTINCT
    `database` AS wiki_db,
    page_id,
    first_value(page_title) OVER w AS page_title,
    first_value(page_namespace) OVER w AS page_namespace
  FROM event.mediawiki_page_move
  WHERE page_id IS NOT NULL AND page_id > 0
    AND page_title IS NOT NULL and LENGTH(page_title) > 0
    AND concat(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0'))
        between
            -- Since history snapshot includes that month we start looking 1 month later
            TO_DATE(concat('${history_snapshot}', '-01')) + INTERVAL 1 MONTH
            and
            -- wikidata snapshot includes that week, so look 1 week later
            TO_DATE('${wikidata_snapshot}') + INTERVAL 7 DAYS

  WINDOW w AS (
    PARTITION BY
      `database`,
      page_id
    ORDER BY
      meta.dt DESC
  )
),

current_page_titles AS (
  SELECT s.wiki_db,
    s.page_id,
    coalesce(u.page_title, s.page_title) as page_title,
    coalesce(u.page_namespace, s.page_namespace) as page_namespace
  FROM snapshot_page_titles s
    LEFT JOIN event_page_titles u
      ON s.wiki_db = u.wiki_db
        AND s.page_id = u.page_id
),


localized_namespace_page_titles AS (
  SELECT
    wiki_db,
    page_id,
    page_title,
    page_namespace,
    CASE WHEN (LENGTH(namespace_localized_name) > 0)
      THEN CONCAT(namespace_localized_name, ':', page_title)
      ELSE page_title
    END AS page_title_localized_namespace
  FROM current_page_titles cpt
    INNER JOIN wmf_raw.mediawiki_project_namespace_map nsm
      ON (
        cpt.wiki_db = nsm.dbname
        AND cpt.page_namespace = nsm.namespace
        AND nsm.snapshot = '${history_snapshot}'
      )
),

wikidata_sitelinks AS (
  SELECT
    id as item_id,
    EXPLODE(siteLinks) AS sitelink
  FROM wmf.wikidata_entity
  WHERE snapshot = '${wikidata_snapshot}'
    AND size(siteLinks) > 0
)

INSERT OVERWRITE TABLE ${destination_table}
  PARTITION(snapshot='${wikidata_snapshot}')
SELECT /*+ COALESCE(${coalesce_partitions}) */
  ws.item_id,
  wiki_db,
  page_id,
  page_title,
  page_namespace,
  page_title_localized_namespace
FROM wikidata_sitelinks ws
INNER JOIN localized_namespace_page_titles lnt
  ON ws.sitelink.site = lnt.wiki_db
    AND REPLACE(ws.sitelink.title, ' ', '_') = page_title_localized_namespace;
