-- Creates table statement for mediawiki_page_history table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_page_history_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE `mediawiki_page_history`(
    wiki_db                               string              COMMENT 'enwiki, dewiki, eswiktionary, etc.',
    page_id                               bigint              COMMENT 'Id of the page, as in the page table.',
    page_artificial_id                    string              COMMENT 'Generated Id for deleted pages without real Id.',
    page_creation_timestamp               string              COMMENT 'Timestamp of the page\'s create event.',
    --page_creation_timestamp               timestamp           COMMENT 'Timestamp of the page\'s create event.',
    page_first_edit_timestamp              string              COMMENT 'Timestamp of the page\'s first revision.',
    --page_first_edit_timestamp              timestamp           COMMENT 'Timestamp of the page\'s first revision.',
    page_title_historical                 string              COMMENT 'Historical page title.',
    page_title                            string              COMMENT 'Page title as of today.',
    page_namespace_historical             int                 COMMENT 'Historical namespace.',
    page_namespace_is_content_historical  boolean             COMMENT 'Whether the historical namespace is categorized as content',
    page_namespace                        int                 COMMENT 'Namespace as of today.',
    page_namespace_is_content             boolean             COMMENT 'Whether the current namespace is categorized as content',
    page_is_redirect                      boolean             COMMENT 'In revision/page events: whether the page is currently a redirect',
    page_is_deleted                       boolean             COMMENT 'Whether the page is rebuilt from a delete event',
    start_timestamp                       string              COMMENT 'Timestamp from where this state applies (inclusive).',
    end_timestamp                         string              COMMENT 'Timestamp to where this state applies (exclusive).',
    --start_timestamp                       timestamp           COMMENT 'Timestamp from where this state applies (inclusive).',
    --end_timestamp                         timestamp           COMMENT 'Timestamp to where this state applies (exclusive).',
    caused_by_event_type                  string              COMMENT 'Event that caused this state (create, move, delete or restore).',
    caused_by_user_id                     bigint              COMMENT 'ID of the user that caused this state.',
    caused_by_user_text                   string              COMMENT 'Name of the user that caused this state.',
    caused_by_anonymous_user              boolean             COMMENT 'Whether the user that caused this state was anonymous',
    inferred_from                         string              COMMENT 'If non-NULL, some fields have been inferred from an inconsistency in the source data.',
    source_log_id                         bigint              COMMENT 'ID of the logging table row that caused this state',
    source_log_comment                    string              COMMENT 'Comment of the logging table row that caused this state',
    source_log_params                     map<string,string>  COMMENT 'Parameters of the logging table row that caused this state, parsed as a map'
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
