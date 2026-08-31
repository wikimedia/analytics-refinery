-- Makes a point-in-time snapshot of page content, as of a cutoff date.
-- The source is the content history table. For each page, this query keeps the most recent revision
-- before the cutoff date.
--
-- `wmf_content.mediawiki_content_current_v1` is not correct for this purpose. That table keeps only
-- the most recent revision of each page. If a person edits a page after the cutoff date, the table
-- has no revision of that page before the cutoff date. The page then disappears from the export.
-- See T431872.
--
-- The result schema is a direct replacement source table for MediawikiDumper.
--
-- The query partitions the output by wiki_id. Consumers read one wiki at a time, so Iceberg removes
-- the partitions of the other wikis. See also the note about write parallelism at the CREATE
-- statement.
--
-- Parameters:
--     source_table      -- Fully qualified name of the content history table to read.
--     destination_table -- Fully qualified name of the table to make. The query removes this table
--                          first, if it exists.
--     cutoff_ds         -- Date in YYYY-MM-DD format. The query excludes revisions at or after this
--                          date. This bound is exclusive. It agrees with the --publish_until bound of
--                          MediawikiDumper.
--
-- Usage:
--     spark35-sql -f create_and_compute_mediawiki_content_current_snapshot.hql \
--         -d source_table=wmf_content.mediawiki_content_history_v1 \
--         -d destination_table=tmp.mediawiki_content_current_as_of_20260701 \
--         -d cutoff_ds=2026-07-01
--
-- Spark settings that matter. The Airflow DAG sets all of these. If you run this file by hand, set
-- them too:
--
--     --conf spark.shuffle.accurateBlockThreshold=1m
--
--         This one is necessary. The write shuffle groups rows by wiki_id. There are about 900 wikis,
--         and their sizes span orders of magnitude, so most blocks of a map task are empty.
--         HighlyCompressedMapStatus then reports one average size for every non-empty block. AQE
--         cannot see the large wikis, does not divide them, and one task gets a full wiki. Without
--         this setting the write stage takes 39 minutes instead of 7.
--
--     --conf spark.sql.shuffle.partitions=8192
--     --conf spark.memory.fraction=0.75            -- the storage pool is almost unused
--     --conf spark.sql.iceberg.locality.enabled=false
--     --executor-memory 24G --executor-cores 2 --conf spark.executor.memoryOverhead=4G
--
--     Do not set spark.reducer.maxReqsInFlight=1. Each reduce task fetches tens of thousands of small
--     blocks, so one request at a time makes the stage wait on network latency.
--
-- Note: run all of this file. If you run only the INSERT statement again, you get the rows two times.
--

DROP TABLE IF EXISTS ${destination_table}
;


CREATE TABLE ${destination_table} (
    `page_id`                      bigint     COMMENT 'The (database) page ID of the page.',
    `page_namespace_id`            int        COMMENT 'The id of the namespace this page belongs to.',
    `page_title`                   string     COMMENT 'The normalized title of the page.',
    `page_redirect_target`         string     COMMENT 'Title of the redirected-to page, if any, NULL otherwise.',
    `user_id`                      bigint     COMMENT 'Id of the user that made the revision.',
    `user_central_id`              bigint     COMMENT 'Global cross-wiki user ID.',
    `user_text`                    string     COMMENT 'Text of the user that made the revision.',
    `user_is_visible`              boolean    COMMENT 'Whether the user that made the revision is visible.',
    `revision_id`                  bigint     COMMENT 'The (database) revision ID.',
    `revision_parent_id`           bigint     COMMENT 'The (database) revision ID of the parent revision.',
    `revision_dt`                  timestamp  COMMENT 'The (database) time this revision was created.',
    `revision_is_minor_edit`       boolean    COMMENT 'True if the editor marked this revision as a minor edit.',
    `revision_comment`             string     COMMENT 'The comment left by the user when this revision was made.',
    `revision_comment_is_visible`  boolean    COMMENT 'Whether the comment of the revision is visible.',
    `revision_size`                bigint     COMMENT 'The sum of the content_size of all content slots.',
    `revision_content_slots`       map<
                                       string,
                                       struct<
                                           content_body:   string,
                                           content_format: string,
                                           content_model:  string,
                                           content_sha1:   string,
                                           content_size:   bigint,
                                           origin_rev_id:  bigint
                                       >
                                   >          COMMENT 'A MAP of all the content slots of this revision.',
    `revision_content_is_visible`  boolean    COMMENT 'Whether revision_content_slots is visible.',
    `wiki_id`                      string     COMMENT 'The wiki ID. For example: enwiki.'
)
USING ICEBERG
-- Iceberg uses the 'hash' write distribution mode for a partitioned table. Each wiki then goes to one
-- write task. AQE divides the large wikis over more tasks, but only if it can see their size. See the
-- note about spark.shuffle.accurateBlockThreshold above.
--
-- Do not give this table a sort order with ALTER TABLE ... WRITE ORDERED BY. A sort order makes
-- Iceberg use the 'range' write distribution mode, and that mode can put all of one wiki in one task.
-- A sort order also gives no benefit to the consumers, because they read one full wiki at a time.
PARTITIONED BY (`wiki_id`)
TBLPROPERTIES (
    'write.target-file-size-bytes'       = '134217728',  -- 128 MiB
    'write.parquet.row-group-size-bytes' = '67108864'    -- 64 MiB
)
;


-- This statement uses an aggregation, and not a ROW_NUMBER window function. A window function moves
-- all revisions of all wikis through a shuffle, and the content columns are large. An aggregation gets
-- map-side partial aggregation. The source table is sorted by wiki_id, page_id and revision_dt, so
-- each input file reduces a page to one row before the shuffle. Measured on all wikis, this shuffle
-- carries 1.3 TiB instead of the tens of TiB of the whole history.
INSERT INTO ${destination_table}
SELECT
    page_id                                     AS page_id,
    latest_revision.page_namespace_id           AS page_namespace_id,
    latest_revision.page_title                  AS page_title,
    latest_revision.page_redirect_target        AS page_redirect_target,
    latest_revision.user_id                     AS user_id,
    latest_revision.user_central_id             AS user_central_id,
    latest_revision.user_text                   AS user_text,
    latest_revision.user_is_visible             AS user_is_visible,
    latest_revision.revision_id                 AS revision_id,
    latest_revision.revision_parent_id          AS revision_parent_id,
    latest_revision.revision_dt                 AS revision_dt,
    latest_revision.revision_is_minor_edit      AS revision_is_minor_edit,
    latest_revision.revision_comment            AS revision_comment,
    latest_revision.revision_comment_is_visible AS revision_comment_is_visible,
    latest_revision.revision_size               AS revision_size,
    latest_revision.revision_content_slots      AS revision_content_slots,
    latest_revision.revision_content_is_visible AS revision_content_is_visible,
    wiki_id                                     AS wiki_id
FROM (
    SELECT
        wiki_id,
        page_id,
        -- The second argument is the sort key. revision_id breaks ties on revision_dt.
        max_by(
            struct(
                page_namespace_id,
                page_title,
                page_redirect_target,
                user_id,
                user_central_id,
                user_text,
                user_is_visible,
                revision_id,
                revision_parent_id,
                revision_dt,
                revision_is_minor_edit,
                revision_comment,
                revision_comment_is_visible,
                revision_size,
                revision_content_slots,
                revision_content_is_visible
            ),
            struct(revision_dt, revision_id)
        ) AS latest_revision
    FROM ${source_table}
    WHERE revision_dt < TIMESTAMP '${cutoff_ds}'
    GROUP BY
        wiki_id,
        page_id
) latest_revision_per_page
;