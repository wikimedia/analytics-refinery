-- Merge git interaction events from a JSON staging directory into an Iceberg table,
--
-- The table is maintained by Refine according to json event schema.
--
-- staging_dir: full HDFS path to the per-run JSON staging directory.
-- target_table: fully-qualified Iceberg table to merge into (e.g. wmf_dev_metrics.git_interaction).
-- coalesce: number of output files written per partition by the MERGE source.
--
-- De-duplication key: (provider, repo, interaction_id, interaction_type, ref)

CREATE OR REPLACE TEMPORARY VIEW raw (
    dt               string,
    interaction_id   string,
    interaction_type string,
    provider         string,
    repo             string,
    ref              string,
    commits array<struct<
      author_email:string, author_name:string, authored_dt:string, committed_dt:string,
      committer_email:string, committer_name:string, parents:array<string>,
      sha:string, subject:string,
      trailers:array<struct<key:string, value:string>>>>
  )
  USING json
  OPTIONS (path '${staging_dir}');

-- Derive the dt range actually present in this batch, as foldable constants.
-- Used below to statically prune the target scan to the covering months(dt)
-- partitions. Bounds come from the source itself, so they always cover every
-- source.dt: pruning can never drop a month that holds a real match.
-- (An inline subquery would not push down to the Iceberg copy-on-write scan.)
DECLARE OR REPLACE VARIABLE dt_lower TIMESTAMP;
DECLARE OR REPLACE VARIABLE dt_upper TIMESTAMP;
SET VARIABLE (dt_lower, dt_upper) = (
  SELECT min(CAST(dt AS timestamp)), max(CAST(dt AS timestamp)) FROM raw
);

MERGE INTO ${target_table} AS target
USING (
  SELECT /*+ COALESCE(${coalesce}) */
      transform(commits, c -> named_struct(
        'author_email',    c.author_email,
        'author_name',     c.author_name,
        'authored_dt',     CAST(c.authored_dt  AS timestamp),
        'committed_dt',    CAST(c.committed_dt AS timestamp),
        'committer_email', c.committer_email,
        'committer_name',  c.committer_name,
        'parents',         c.parents,
        'sha',             c.sha,
        'subject',         c.subject,
        'trailers',        c.trailers
      )) AS commits,
      CAST(dt AS timestamp) AS dt,
      interaction_id,
      interaction_type,
      provider,
      ref,
      repo
  FROM raw
) AS source
ON  target.provider         = source.provider
AND target.repo             = source.repo
AND target.interaction_id   = source.interaction_id
AND target.interaction_type = source.interaction_type
AND target.ref              <=> source.ref
AND target.dt               = source.dt
-- Static, foldable bounds: prune the target scan to the covering months(dt).
AND target.dt               >= dt_lower
AND target.dt               <= dt_upper
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
