-- Creates table statement for wikidata_item_page_link table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_wikidata_item_page_link_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE `wikidata_item_page_link` (
    `item_id`                         string   COMMENT 'The wikidata item_id (Q32753077 for instance)',
    `wiki_db`                         string   COMMENT 'The db project of the page the wikidata item links to',
    `page_id`                         bigint   COMMENT 'The id of the page the wikidata item links to',
    `page_title`                      string   COMMENT 'The title of the page the wikidata item links to',
    `page_namespace`                  int      COMMENT 'The namespace of the page the wikidata item links to',
    `page_title_localized_namespace`  string   COMMENT 'The title with localized namespace header of the page the wikidata item links to'
)
COMMENT
    'See most up to date documentation at https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/Wikidata_item_page_link'
PARTITIONED BY (
    `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM-DD for regular weekly imports)'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/wikidata/item_page_link'
;

