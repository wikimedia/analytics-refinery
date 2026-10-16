--
-- Creates the phabricator_interaction Iceberg table: a log of Phabricator
-- Maniphest transactions (comments, status/assignment/title/priority
-- changes, ...), each carrying a point-in-time snapshot of the
-- task it acts on. De-duplication key: (provider, interaction_id), where
-- interaction_id is the globally unique transaction PHID.
--
-- Usage:
--     spark3-sql -f create_phabricator_interaction_table.hql
--

CREATE TABLE IF NOT EXISTS wmf_dev_metrics.phabricator_interaction (
    `author` struct<
        irc_nick: string COMMENT 'Self-declared IRC nick (custom.irc_nick), where set.',
        mediawiki_username: string COMMENT 'Linked MediaWiki/Wikimedia username, where set.',
        real_name: string COMMENT 'Display/real name, where set (often empty).',
        username: string COMMENT 'Phabricator account username for a user; the display name (e.g. Herald) for other actor types.',
        type: string COMMENT 'Kind of actor, from the author_phid prefix: user (PHID-USER), application (PHID-APPS, e.g. Herald), repository_identity (PHID-RIDT, a commit identity), or other. Filter on user for metrics about people; bots using regular accounts (e.g. gerritbot) are still user.'
    >
        COMMENT 'Resolved identity of the actor (author_phid): accounts via user.search, other actors (Herald, repository identities, ...) via phid.query. A point-in-time snapshot like the task (a username can change). Absent on tombstones; otherwise always carries type, plus the resolved fields when the PHID resolved. Phabricator does not expose user email, so this is username-level identity only. mediawiki_username links to the person\'s Wikimedia identity.',
    `author_phid` string
        COMMENT 'PHID of the person or bot performing the transaction (the actor of the interaction). Identifies a Phabricator account, not necessarily a Wikimedia-wide identity. See author for its resolved username.',
    `comments` string
        COMMENT 'Comment body, where present. May contain sensitive text.',
    `dt` timestamp
        COMMENT 'ISO-8601 timestamp, in UTC, of when the interaction occurred — the transaction\'s creation time reported by Phabricator — not when it was collected.',
    `interaction_id` string
        COMMENT 'Stable identifier of the interaction; the de-duplication key together with provider. For interaction sourced from a Maniphest transaction this is the transaction PHID (PHID-XACT-…), which is globally unique.',
    `interaction_type` string
        COMMENT 'The kind of interaction being recorded, such as: core:comment, status, title, priority, core:columns, or reassign. A short free-form string rather than an enum, so new transaction types are retained without a schema change. The reserved value "collector:redacted" marks a synthetic redaction tombstone: the task_id is no longer visible and all its rows should be removed downstream.',
    `new_value` string
        COMMENT 'New value for the changed field, where supplied. Values that are structured in the API are serialised as JSON strings.',
    `old_value` string
        COMMENT 'Previous value for the changed field, where supplied. Values that are structured in the API are serialised as JSON strings.',
    `provider` string
        COMMENT 'Identifier of the source host, e.g. wmf_phabricator. So a second Phorge instance can be added without a schema change. Paired with interaction_id for de-duplication, though the transaction PHID is already globally unique.',
    `summary` string
        COMMENT 'API-supplied transaction title/summary, where present, e.g. "Closed T12345". Named summary to avoid colliding with the task entity\'s own title.',
    `task` struct<
        title: string COMMENT 'Current task title.',
        author_phid: string COMMENT 'PHID of the task creator. Identifies a Phabricator account, not necessarily a Wikimedia-wide identity.',
        closer_phid: string COMMENT 'PHID of the account that closed the task, where closed.',
        date_closed: timestamp COMMENT 'Task close time, in UTC, where closed.',
        date_created: timestamp COMMENT 'Task creation time, in UTC.',
        date_modified: timestamp COMMENT 'Most recent task modification time, in UTC.',
        edit_policy: string COMMENT 'Current edit policy: a well-known token (e.g. users) or a PHID. Part of the access-restriction signal.',
        owner_phid: string COMMENT 'PHID of the current assignee/owner, where assigned.',
        points: string COMMENT 'Story points, where set; serialised as a string.',
        priority: string COMMENT 'Current task priority, e.g. high.',
        projects: array<string> COMMENT 'PHIDs of the projects/tags on the task (teams, workboards), from the maniphest.search projects attachment. Enables per-project activity metrics without resolving names at collection.',
        space_phid: string COMMENT 'PHID of the space the task lives in. Spaces gate visibility; part of the access-restriction signal.',
        status: string COMMENT 'Current task status, e.g. open or resolved.',
        subtype: string COMMENT 'Task subtype, e.g. default or security.',
        task_phid: string COMMENT 'Stable Phabricator task identifier.',
        view_policy: string COMMENT 'Current view policy: a well-known token (e.g. public, users) or a PHID. The primary access-restriction signal — a downstream job can remove a task\'s rows when this is not a public value.'
    >
        COMMENT 'The task entity this interaction acts on: a point-in-time snapshot of the Maniphest task\'s metadata as seen at collection. Optional (absent/null in the Data Lake) when the snapshot could not be captured; when present it carries the full required metadata.',
    `task_id` bigint
        COMMENT 'Numeric Maniphest task ID of the task this interaction acts on; the public task identifier and the parent container that groups a task\'s interactions.',
    `transaction_id` bigint
        COMMENT 'Numeric transaction ID. Not stable enough to key on alone.'
)
USING ICEBERG
PARTITIONED BY (months(`dt`))
TBLPROPERTIES (
    'write.distribution-mode' = 'hash',
    -- Goal of the table maintenance rewrite_data_files compaction, and upper
    -- bound for writes. Write tasks are sized by
    -- spark.sql.iceberg.advisory-partition-size in the import DAG.
    'write.target-file-size-bytes' = '268435456'
)
LOCATION 'hdfs://analytics-hadoop/wmf/data/wmf_dev_metrics/phabricator_interaction'
;


-- Set the table's write order so subsequent MERGE writes cluster files by
-- (provider, task_id, dt). Read optimization, and it also narrows the set of
-- files a redaction DELETE (keyed on provider, task_id) must rewrite under
-- copy-on-write. A separate statement because WRITE ORDERED BY is an ALTER, not
-- expressible in CREATE TABLE. Idempotent: only alters table metadata.
ALTER TABLE wmf_dev_metrics.phabricator_interaction WRITE ORDERED BY provider, task_id, dt;
