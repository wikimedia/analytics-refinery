--
-- Creates the git_interaction Iceberg table: an append-only observation log of
-- git interactions (e.g. commits becoming visible) across organization repos.
-- De-duplication key: (provider, repo, interaction_id).
--
-- Usage:
--     spark3-sql -f create_git_interaction_table.hql
--

CREATE TABLE IF NOT EXISTS wmf_dev_metrics.git_interaction (
    `commits` array<struct<
        author_email: string COMMENT 'Commit author email from git metadata; may be anonymised or non-deliverable.',
        author_name: string COMMENT 'Commit author name from git metadata.',
        authored_dt: timestamp COMMENT 'Commit author timestamp from git metadata.',
        committed_dt: timestamp COMMENT 'Committer timestamp from git metadata.',
        committer_email: string COMMENT 'Committer email from git metadata.',
        committer_name: string COMMENT 'Committer name from git metadata. On Gerrit this is rewritten to the submitter on merge, so it can differ from the author.',
        parents: array<string> COMMENT 'Parent commit SHAs in order. A length of 2 or more indicates a merge commit, letting consumers apply a merge-inclusion policy downstream.',
        sha: string COMMENT 'Git commit object ID (full 40- or 64-char hex SHA).',
        subject: string COMMENT 'Commit-message subject (first line).',
        trailers: array<struct<
            key: string COMMENT 'Trailer key, for example Change-Id or Bug.',
            value: string COMMENT 'Raw trailer value.'
        >> COMMENT 'Ordered commit-message trailers, preserving unknown and repeated keys in addition to the named convenience fields.'
    >>
        COMMENT 'The commit entities this interaction references. Optional: absent (null in the Data Lake) when the interaction references no commits. In v1 this holds exactly one commit, but the model allows an interaction to reference several (e.g. a push). When present it has at least one entry.',
    `dt` timestamp
        COMMENT 'ISO-8601 timestamp, in UTC, of when the interaction occurred (event time), not when it was collected. For interaction_type=commit this is the commit\'s committed_dt (the git committer timestamp), so the same value is emitted whether a record comes from the initial backfill or from incremental collection.',
    `interaction_id` string
        COMMENT 'Stable identifier of the interaction within (provider, repo); the third part of the de-duplication key. For interaction_type=commit this is the commit sha (see commits[].sha). For interaction types that reference several or no commits (e.g. push, merge, review), set it from a provider-native id or a deterministic hash of the interaction\'s defining fields, so re-emitted records collide rather than duplicate.',
    `interaction_type` string
        COMMENT 'Kind of interaction being recorded. In v1 the collector iterates a repository and emits one interaction per observed commit, so this is "commit"; the model leaves room for other repo interactions later.',
    `provider` string
        COMMENT 'Identifier of the source host, e.g. gerrit, github, wmf_gitlab. A short free-form string rather than an enum, so activity from further providers can be added without a schema change. Paired with repo to identify a repository.',
    `ref` string
        COMMENT 'Git ref/branch the interaction was observed on, where known, e.g. refs/heads/main. Optional; commit reachability is not guaranteed to be stable.',
    `repo` string
        COMMENT 'Repository path within the provider, without the provider prefix, e.g. mediawiki/core. Identity is (provider, repo); the provider field disambiguates repositories that share a path on different hosts.'
)
USING ICEBERG
PARTITIONED BY (months(`dt`))
LOCATION 'hdfs://analytics-hadoop/wmf/data/wmf_dev_metrics/git_interaction'
;


-- Set the table's write order so subsequent MERGE writes cluster files by
-- (repo, ref, dt). Read optimization. A separate statement because WRITE
-- ORDERED BY is an ALTER, not expressible in CREATE TABLE.
-- Idempotent: only alters table metadata, no data rewrite.
ALTER TABLE wmf_dev_metrics.git_interaction WRITE ORDERED BY repo, ref, dt;
