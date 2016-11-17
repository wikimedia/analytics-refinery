-- Creates table statement for mediawiki_page_history table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_page_history_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE `wmf.mediawiki_page_history`(
    wiki_db                             string      COMMENT 'enwiki, dewiki, eswiktionary, etc.',
    page_id                             bigint      COMMENT 'Id of the page, as in the page table.',
    page_id_artificial                  string      COMMENT 'Generated Id for deleted pages without real Id.',
    page_creation_timestamp             string      COMMENT 'Timestamp of the page\'s first revision.',
    page_title                          string      COMMENT 'Historical page title.',
    page_title_latest                   string      COMMENT 'Page title as of today.',
    page_namespace                      int         COMMENT 'Historical namespace.',
    page_namespace_is_content           boolean     COMMENT 'Whether the historical namespace is categorized as content',
    page_namespace_latest               int         COMMENT 'Namespace as of today.',
    page_namespace_is_content_latest    boolean     COMMENT 'Whether the current namespace is categorized as content',
    page_is_redirect_latest             boolean     COMMENT 'In revision/page events: whether the page is currently a redirect',
    start_timestamp                     string      COMMENT 'Timestamp from where this state applies (inclusive).',
    end_timestamp                       string      COMMENT 'Timestamp to where this state applies (exclusive).',
    caused_by_event_type                string      COMMENT 'Event that caused this state (create, move, delete or restore).',
    caused_by_user_id                   bigint      COMMENT 'ID from the user that caused this state.',
    inferred_from                       string      COMMENT 'If non-NULL, some fields have been inferred from an inconsistency in the source data.'
)
COMMENT
  'See most up to date documentation at https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Mediawiki_page_history'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/wmf/mediawiki/page_history'
;
