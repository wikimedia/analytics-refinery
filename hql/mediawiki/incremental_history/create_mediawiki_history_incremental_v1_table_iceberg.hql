-- Creates table statement for mediawiki_history_incremental_v1.
--
-- Delivers MediaWiki History data on a daily cadence via two writers:
--
--   source='events'   Daily delta job (MWHistoryDeltaWriter).  Reads
--                     mediawiki.page_change events.  Best-effort quality.
--
--   source='snapshot' Monthly merge job (MWHistorySnapshotMerger).  Projects
--                     wmf.mediawiki_history after each monthly rebuild.
--                     Full-history quality, identical semantics to
--                     wmf.mediawiki_history.
--
-- Consumer story: query this table for the freshest available data.  Recent
-- days hold source='events' rows; older days hold source='snapshot' rows once
-- the monthly merge has run.  Use WHERE source = 'snapshot' for strict
-- monthly-only semantics.
--
-- Note: some fields may not yet hold their final value at query time.
-- revision_tags and revision_deleted_parts on source='events' rows are
-- NULL until the daily tags/visibility writers have processed the same
-- day.  Revert fields (revision_is_identity_reverted,
-- revision_first_identity_reverting_revision_id,
-- revision_seconds_to_identity_revert, revision_is_identity_revert) may
-- be patched in-place on both source values when a reverting revision is
-- processed.  All such fields should be read as "best available as of
-- last daily run."
--
-- Write mode: copy-on-write (COW).  MERGE INTO rewrites affected data files
-- in full.  Simpler operational profile; MOR can be evaluated later if write
-- amplification on the monthly snapshot merge becomes a bottleneck.
--
-- Partitioning: months(event_timestamp).  Day-level partitioning was tested
-- and produced ~32k files at 0.4 MB median (vs 128 MB target) because the
-- monthly snapshot merger writes all of history (~9 000 daily partitions) and
-- each shuffle task writes a separate file per partition.  Monthly granularity
-- reduces partition count from ~9 000 to ~300 and keeps file sizes near
-- target.  Note: spark.sql.iceberg.locality.enabled must remain true on
-- colocated YARN+HDFS clusters; false is only a temporary workaround while
-- file counts are elevated.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_mediawiki_history_incremental_v1_table_iceberg.hql \
--         --database wmf_mediawiki                                            \
--         -d location=/wmf/data/wmf_mediawiki/mediawiki_history_incremental_v1
--

CREATE EXTERNAL TABLE IF NOT EXISTS `mediawiki_history_incremental_v1` (
    `source`                                                           string        COMMENT 'Row provenance: events (daily delta) or snapshot (monthly merge).',

    `wiki_id`                                                          string        COMMENT 'Wiki identifier, e.g. enwiki, dewiki, eswiktionary.',

    `event_entity`                                                     string        COMMENT 'Entity type: revision, user, or page.',
    `event_type`                                                       string        COMMENT 'Event sub-type: create, edit, move, delete, etc.',
    `event_timestamp`                                                  timestamp     COMMENT 'When this event occurred.',
    `event_user_id`                                                    bigint        COMMENT 'Local MediaWiki user ID of the actor; NULL for anonymous users.',
    `event_user_central_id`                                            bigint        COMMENT 'Global CentralAuth user ID of the actor; NULL for anonymous users.',
    `event_user_text_historical`                                       string        COMMENT 'Username or IP at the time of the event.',
    `event_user_is_bot_by_historical`                                  array<string> COMMENT 'Bot classification methods at event time: name and/or group.',
    `event_user_is_anonymous`                                          boolean       COMMENT 'True if the actor had no local user account at event time.',
    `event_user_is_temporary`                                          boolean       COMMENT 'True if the actor was a temporary (auto-created) account.',
    `event_user_is_permanent`                                          boolean       COMMENT 'True if the actor was a permanent registered account.',
    `event_user_registration_timestamp`                                timestamp     COMMENT 'When the actor account was registered; NULL for anonymous users.',
    `event_user_revision_count`                                        bigint        COMMENT 'Edit count of the actor at event time.',
    `event_user_groups_historical`                                     array<string> COMMENT 'Groups held by the actor at event time (before the change for altergroups events). NULL for revision and page events.',
    `event_user_is_cross_wiki`                                         boolean       COMMENT 'True if the actor is a CentralAuth user editing while logged out (username format IP>GlobalUsername). NULL (not FALSE) when the actor username is absent from the event (e.g. system-created user accounts where performer text is not populated).',

    -- user_* columns describe the user being acted upon (the entity) for user events
    -- (create, rename, altergroups). NULL for revision and page event rows.
    `user_id`                                                          bigint        COMMENT 'Local MediaWiki user ID of the user being created/renamed/altered.',
    `user_central_id`                                                  bigint        COMMENT 'Global CentralAuth user ID of the user being created/renamed/altered. NULL for non-user events.',
    `user_text_historical`                                             string        COMMENT 'Username of the user after the event: new name for renames, current name otherwise.',
    `user_is_anonymous`                                                boolean       COMMENT 'True if the user had no local account (always false for user events).',
    `user_is_temporary`                                                boolean       COMMENT 'True if the user being acted upon was a temporary account.',
    `user_is_permanent`                                                boolean       COMMENT 'True if the user being acted upon was a permanent registered account.',
    `user_groups_historical`                                           array<string> COMMENT 'Groups held by the user after the change (for altergroups), or at creation.',
    `user_is_bot_by_historical`                                        array<string> COMMENT 'Bot classification of the user based on user_groups_historical: name and/or group.',
    `user_is_created_by_self`                                          boolean       COMMENT 'True if the user registered their own account (performer == user, not autocreate). NULL for non-create events.',
    `user_is_created_by_system`                                        boolean       COMMENT 'True if the account was autocreated by the system (SSO/CentralAuth). NULL for non-create events.',
    `user_is_created_by_peer`                                          boolean       COMMENT 'True if a distinct admin created the account (performer != user, not autocreate). NULL for non-create events.',

    -- page_* columns describe the page being acted upon. NULL for user event rows.
    `page_id`                                                          bigint        COMMENT 'Page ID at event time.',
    `page_title_historical`                                            string        COMMENT 'Page title (without namespace prefix) at event time.',
    `page_namespace_historical`                                        int           COMMENT 'Page namespace ID at event time.',
    `page_namespace_is_content_historical`                             boolean       COMMENT 'True if page_namespace_historical is a content namespace.',
    `page_is_deleted`                                                  boolean       COMMENT 'True if the page was in deleted state at this event. Set at event time for page events; back-patched for revision rows when a page delete/undelete arrives. NULL for user events.',

    -- revision_* columns describe the revision. NULL for page and user event rows
    -- unless noted otherwise in the individual column comment.
    `revision_id`                                                      bigint        COMMENT 'Revision ID; NULL for non-revision events.',
    `revision_parent_id`                                               bigint        COMMENT 'Parent revision ID; NULL for page-creation revisions.',
    `revision_minor_edit`                                              boolean       COMMENT 'True if the editor flagged this as a minor edit.',
    `revision_text_bytes`                                              bigint        COMMENT 'Uncompressed byte size of the revision text.',
    `revision_text_bytes_diff`                                         bigint        COMMENT 'Byte delta vs. parent revision; NULL for page-creation revisions.',
    `revision_text_sha1`                                               string        COMMENT 'SHA-1 of the concatenated slot content (all-slots hash).',
    `revision_deleted_parts`                                           array<string> COMMENT 'Visibility-suppressed components: text, comment, and/or user. Populated by the daily visibility writer; NULL until visibility data arrives. NULL for page and user events.',
    `revision_is_identity_reverted`                                    boolean       COMMENT 'True if a later revision restored the page to the state before this one. Source=events rows: FALSE for fresh revisions (rev_dt=today, non-null sha1) — a fresh revision cannot already be reverted; upgraded to TRUE when the reverter arrives. NULL for late-arriving or null-sha1 revisions until the monthly snapshot provides the authoritative value.',
    `revision_first_identity_reverting_revision_id`                    bigint        COMMENT 'ID of the first revision that identity-reverted this one; NULL if not reverted.',
    `revision_seconds_to_identity_revert`                              bigint        COMMENT 'Seconds between this revision and the first identity revert; NULL if not reverted.',
    `revision_is_identity_revert`                                      boolean       COMMENT 'True if this revision itself is an identity revert (restores a prior page state). Source=events rows: FALSE for fresh revisions (rev_dt=today, non-null sha1) where no revert was detected; NULL for late-arriving or null-sha1 revisions.',
    `revision_is_deleted_by_page_deletion`                             boolean       COMMENT 'True if this revision is in deleted state because its page was deleted (as opposed to an explicit RevisionDelete action). Back-patched when a page delete/undelete event arrives. NULL for page and user events.',
    `revision_tags`                                                    array<string> COMMENT 'Change tags applied to this revision. Populated asynchronously by the daily tags writer; NULL until tags arrive.',

    `event_meta_id`                                                    string        COMMENT 'Delivery UUID (meta.id) used as the MERGE key for page and user events. NULL for revision rows and all snapshot rows.',
    `control_map`                                                      map<string,string> COMMENT 'Internal, do not use for queries. Per-stream rerun guard. Each MERGE writes only its own key(s); other keys are preserved. Keys: revision_update_dt (M1), revert_patch_dt (M2), tags_update_dt (M3), visibility_update_dt (M4), page_meta_id + page_update_dt (M5), page_deletion_dt (M6), user_meta_id + user_update_dt (M7). NULL for source=snapshot rows.',
    `row_update_dt`                                                    timestamp     COMMENT 'Incremental-read watermark of the run that last wrote this row. source=events: the daily run data_interval_end (run day + 1, UTC), advanced via GREATEST so back-patches and out-of-order reruns never lower it. source=snapshot: the last second of the snapshot month (one second before the monthly reconcile data_interval_end). The daily pipeline blocks on the 1st until the reconcile lands, so the reconcile orders after all daily runs of the snapshot month and before the first daily run of the next month, making the monthly authoritative refresh a single in-order, monotonic event on this watermark. Consumers read deltas via WHERE row_update_dt >= last_watermark (pair with an event_timestamp filter for partition pruning).'
)
USING ICEBERG
TBLPROPERTIES (
    -- Iceberg v2 enables row-level delete support and future MOR migration.
    'format-version'                   = '2',
    -- Parquet column encoding; zstd gives the best size/speed tradeoff at WMF scale.
    'write.parquet.compression-codec'  = 'zstd',
    -- 128 MB target keeps file counts manageable per monthly partition.
    'write.target-file-size-bytes'     = '134217728'
)
PARTITIONED BY (months(event_timestamp))
LOCATION '${location}'
;
