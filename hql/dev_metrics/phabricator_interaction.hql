-- Merge Phabricator interactions from a JSON staging directory into an
-- Iceberg table.
--
-- The target table is created by create_phabricator_interaction_table.hql
--
-- Upsert key:  (provider, interaction_id) -- interaction_id is the transaction
--              PHID (globally unique).
-- Redaction:   rows with interaction_type = 'collector:redacted' are tombstones,
--              NOT stored. They mark a task that is no longer visible (deleted or
--              access-restricted); every existing row for their (provider,
--              task_id) is deleted first, so no trace of that task remains.
--              The rolling reconciliation (collector --reconcile-slice) relies on
--              this: it tombstones a slice of task IDs and re-emits their full
--              current history, so step 1 + step 2 replace those tasks.
-- Dedup:       a task can be both in the modified-since window and in the
--              reconcile slice, so an interaction can be staged twice. Step 2
--              keeps one row per (provider, interaction_id), the one with the
--              most recent task snapshot.
--
-- Parameters:
--     staging_dir   -- Full HDFS path to the per-run JSON staging directory.
--     target_table  -- Fully-qualified Iceberg table to merge into
--                      (e.g. wmf_dev_metrics.phabricator_interaction).
--
-- Usage:
--     spark3-sql -f phabricator_interaction.hql                      \
--         -d staging_dir=hdfs:///wmf/data/phabricator_interaction/run \
--         -d target_table=wmf_dev_metrics.phabricator_interaction

CREATE OR REPLACE TEMPORARY VIEW raw (
    dt               string,
    provider         string,
    interaction_id   string,
    interaction_type string,
    author_phid      string,
    task_id          bigint,
    transaction_id   bigint,
    summary          string,
    comments         string,
    old_value        string,
    new_value        string,
    author           struct<
      username:string, real_name:string, mediawiki_username:string, irc_nick:string,
      type:string>,
    task             struct<
      task_phid:string, title:string, author_phid:string,
      date_created:string, date_modified:string, date_closed:string,
      status:string, priority:string, subtype:string, points:string,
      owner_phid:string, closer_phid:string,
      view_policy:string, edit_policy:string, space_phid:string,
      projects:array<string>>
  )
  USING json
  OPTIONS (path '${staging_dir}');

-- Partition-pruning bounds (SPARK 4+ ONLY, currently disabled).
-- Derive the dt range of the real interactions in this batch into session
-- variables, then reference them as foldable constants in the upsert ON clause
-- (step 2) to statically prune the target scan to the covering months(dt)
-- partitions. Bounds come from the source itself, so they always cover every
-- upserted source.dt; and because interaction_id (the upsert key) carries an
-- immutable transaction dt, a matching target row shares that dt -- so pruning
-- can never drop a real match. (An inline subquery would not push down to the
-- Iceberg copy-on-write scan.) The redaction delete in step 1 is NOT bounded:
-- it must remove a task's rows across all of history.
--
-- DECLARE OR REPLACE VARIABLE dt_lower TIMESTAMP;
-- DECLARE OR REPLACE VARIABLE dt_upper TIMESTAMP;
-- SET VARIABLE (dt_lower, dt_upper) = (
--   SELECT min(CAST(dt AS timestamp)), max(CAST(dt AS timestamp))
--   FROM raw
--   WHERE interaction_type <> 'collector:redacted'
-- );

-- 1) Redaction tombstones: delete every stored row for tasks that are no longer visible.
--    Keyed on (provider, task_id) because a tombstone's interaction_id does not match the
--    transaction rows it is meant to remove.
MERGE INTO ${target_table} AS target
USING (
  SELECT DISTINCT provider, task_id
  FROM raw
  WHERE interaction_type = 'collector:redacted'
) AS redacted
ON  target.provider = redacted.provider
AND target.task_id  = redacted.task_id
WHEN MATCHED THEN DELETE;

-- 2) Upsert the real interactions (everything that is not a tombstone).
MERGE INTO ${target_table} AS target
USING (
  SELECT
      CASE WHEN author IS NULL THEN NULL ELSE named_struct(
        'irc_nick',           author.irc_nick,
        'mediawiki_username', author.mediawiki_username,
        'real_name',          author.real_name,
        'username',           author.username,
        'type',               author.type
      ) END AS author,
      author_phid,
      comments,
      CAST(dt AS timestamp) AS dt,
      interaction_id,
      interaction_type,
      new_value,
      old_value,
      provider,
      summary,
      CASE WHEN task IS NULL THEN NULL ELSE named_struct(
        'title',         task.title,
        'author_phid',   task.author_phid,
        'closer_phid',   task.closer_phid,
        'date_closed',   CAST(task.date_closed   AS timestamp),
        'date_created',  CAST(task.date_created  AS timestamp),
        'date_modified', CAST(task.date_modified AS timestamp),
        'edit_policy',   task.edit_policy,
        'owner_phid',    task.owner_phid,
        'points',        task.points,
        'priority',      task.priority,
        'projects',      task.projects,
        'space_phid',    task.space_phid,
        'status',        task.status,
        'subtype',       task.subtype,
        'task_phid',     task.task_phid,
        'view_policy',   task.view_policy
      ) END AS task,
      task_id,
      transaction_id
  FROM (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY provider, interaction_id
            ORDER BY task.date_modified DESC NULLS LAST
        ) AS rn
    FROM raw
    WHERE interaction_type <> 'collector:redacted'
  ) AS deduplicated
  WHERE rn = 1
) AS source
ON  target.provider       = source.provider
AND target.interaction_id = source.interaction_id
-- Static, foldable bounds (SPARK 4+): prune the target scan to the covering
-- months(dt). Enable together with the DECLARE/SET VARIABLE block above.
-- AND target.dt             >= dt_lower
-- AND target.dt             <= dt_upper
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
