-- Creates table statement for table containing list of wikis to be ingested monthly.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark-sql -f create_ingestion_wikis_table.hql \
--         --database wmf \
--         -d location = /wmf/data/wmf/mediawiki/database/ingestion_wikis
--
-- The first data used to populate this table was taken from
-- /wmf/refinery/current/static_data/mediawiki/grouped_wikis/grouped_wikis.csv


CREATE TABLE `ingestion_wikis` (
  `wiki_db` string COMMENT 'wiki database present in cloud replica',
  `wiki_group` int COMMENT 'wiki group number',
  `size` int COMMENT 'Edit size'
)
USING CSV
OPTIONS (
  header 'false',
  delimiter ',',
  compression 'none'
)
LOCATION '${location}'
;