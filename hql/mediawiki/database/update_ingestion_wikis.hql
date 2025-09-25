-- This query updates the data file for '${ingestion_wikis_table}' table
-- Usage
--     spark3-sql -f update_ingestion_wikis.hql \
--         -d project_namespace_table=wmf_raw.mediawiki_project_namespace_map \
--         -d ingestion_wikis_table=wmf.ingestion_wikis \
--         -d snapshot=2025-09
--

-- Spark settings to avoid compressed output file.
SET spark.sql.csv.compression = none;
SET spark.sql.sources.compression.codec = none;

-- Materialize the merged updated wikis data directly into temp table:
--   We use a temporary table because spark cannot read from and write to
--   the same table (i.e. ingestion_wikis_table) due to it's lazy evaluation.
--   The insert overwrite will drop the table we are reading from. To avoid
--   this we need a temp table to hold the data temporarily.
--
DROP TABLE IF EXISTS wmf.temp_ingestion_wikis;

CREATE TABLE wmf.temp_ingestion_wikis AS
SELECT
  COALESCE(pjs.wiki_db, curr_sqplist.wiki_db) AS wiki_db,
  COALESCE(curr_sqplist.wiki_group, 15) AS wiki_group,
  COALESCE(curr_sqplist.size, 1) AS size
FROM
  (
    SELECT DISTINCT dbname AS wiki_db
    FROM ${project_namespace_table}
    WHERE
      wiki_is_closed = false
      AND wiki_has_cloud_replica = true
      AND snapshot = '${snapshot}'
  ) AS pjs
  FULL OUTER JOIN ${ingestion_wikis_table} AS curr_sqplist
    ON pjs.wiki_db = curr_sqplist.wiki_db;

-- Overwrite the original table using the data from the temporary table
INSERT OVERWRITE TABLE ${ingestion_wikis_table}
SELECT /*+ COALESCE(1) */
  wiki_db,
  wiki_group,
  size
FROM
  wmf.temp_ingestion_wikis
ORDER BY
  wiki_group, wiki_db;

-- Drop the temp table
DROP TABLE wmf.temp_ingestion_wikis;