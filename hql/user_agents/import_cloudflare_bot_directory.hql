-- Import a snapshot of the Cloudflare Radar bot directory from JSONL received.
--
-- Parameters:
--     destination_table -- Fully qualified name of the destination table.
--     snapshot_day      -- Snapshot day string in YYYY-MM-DD format
--     input_json_path   -- Path to input JSONL file(s)
--
-- Usage:
--     spark3-sql -f import_cloudflare_bot_directory.hql \
--         -d destination_table=wmf_traffic.cloudflare_bot_directory \
--         -d input_json_path=hdfs:///wmf/tmp/analytics/cloudflare_bot_directory/2026-08-03/bots.jsonl \
--         -d snapshot_day=2026-08-03

DELETE FROM ${destination_table}
WHERE `snapshot_day` = DATE('${snapshot_day}');

INSERT INTO TABLE ${destination_table}
SELECT /*+ COALESCE(1) */
    slug,
    name AS bot_name,
    kind AS bot_kind,
    `operator` AS bot_operator,
    operatorUrl AS bot_operator_url,
    category AS bot_category,
    description AS bot_description,
    userAgentPatterns AS user_agent_patterns,
    userAgents AS user_agents,
    DATE('${snapshot_day}') AS snapshot_day
FROM json.`${input_json_path}`;
