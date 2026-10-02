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
    `$schema`        string,
    dt               string,
    interaction_id   string,
    interaction_type string,
    provider         string,
    repo             string,
    ref              string,
    meta             struct<stream:string, dt:string>,
    commits array<struct<
      author_email:string, author_name:string, authored_at:string, committed_at:string,
      committer_email:string, committer_name:string, parents:array<string>,
      sha:string, subject:string,
      trailers:array<struct<key:string, value:string>>>>
  )
  USING json
  OPTIONS (path '${staging_dir}');

MERGE INTO ${target_table} AS target
USING (
  SELECT /*+ COALESCE(${coalesce}) */
      `$schema`,
      transform(commits, c -> named_struct(
        'author_email',    c.author_email,
        'author_name',     c.author_name,
        'authored_at',     CAST(c.authored_at  AS timestamp),
        'committed_at',    CAST(c.committed_at AS timestamp),
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
      named_struct(
        'domain',     CAST(NULL AS string),
        'dt',         CAST(meta.dt AS timestamp),
        'id',         CAST(NULL AS string),
        'request_id', CAST(NULL AS string),
        'stream',     meta.stream,
        'uri',        CAST(NULL AS string)
      ) AS meta,
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
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;
