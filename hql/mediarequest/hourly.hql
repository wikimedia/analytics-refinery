-- Parameters:
--  refinery_hive_shaded
--  source_table             -- Fully qualified table name to compute the
--                              aggregation for.
--  destination_table        -- Fully qualified table name to fill in
--                              aggregated values.
--  mediawiki_file_table     -- Fully qualified mediawiki_file dimension table
--                              (e.g. wmf_raw.mediawiki_file). Used as ground
--                              truth for media_classification.
--  mediawiki_filetypes_table -- Fully qualified mediawiki_filetypes dimension
--                              table (e.g. wmf_raw.mediawiki_filetypes).
--  mw_snapshot              -- YYYY-MM snapshot of mediawiki_file/filetypes to
--                              read. By policy this is the previous month
--                              relative to ${year}-${month} (e.g. 2026-03 hour
--                              -> 2026-02 snapshot). The Airflow DAG computes
--                              this from data_interval_start.
--  coalesce_partitions      -- Number of files in the result.
--  year                     -- year of partition to compute aggregation for.
--  month                    -- month of partition to compute aggregation for.
--  day                      -- day of partition to compute aggregation for.
--  hour                     -- hour of partition to compute aggregation for.
--
-- Behaviour:
--   media_classification is first derived from the URL extension via the
--   parse_media_file_url UDF (existing behaviour). It is then optionally
--   corrected against MediaWiki's file + filetypes tables ("ground truth"
--   for the originally uploaded file) under an agree-first override policy:
--     * If the UDF value is already 'document', keep it (URL extension is
--       straight forward for pdf/djvu/srt/txt; do not override).
--     * If the DB has no opinion (no join match, file unmapped, or
--       ft_media_type='UNKNOWN') -> keep the UDF value.
--     * If the DB-derived bucket equals the UDF value -> keep the UDF value.
--     * Otherwise (true disagreement) -> use the DB-derived bucket.
--   The override only ever produces a value from the AQS-documented bucket
--   set {image, audio, video, document, other}; See:
--   https://doc.wikimedia.org/generated-data-platform/aqs/analytics-api/reference/media-files.html#media-types
--
--   Per-wiki join: the first two segments of base_name (project family +
--   site key) are mapped to the matching MediaWiki wiki_db (e.g.
--   /wikipedia/en/ -> 'enwiki', /wikipedia/commons/ -> 'commonswiki',
--   /wikimedia/pl/ -> 'plwikimedia', /wikibooks/de/ -> 'dewikibooks').
--   `wikipedia` is the only family that appends 'wiki'; the other eight
--   families (wikimedia, wikibooks, wiktionary, wikiquote, wikisource,
--   wikiversity, wikivoyage, wikinews) append their own name. Site keys
--   with hyphens are normalized to underscores ('zh-yue' -> 'zh_yuewiki').
--   Rows whose base_name does not match a recognized family (math/score/
--   timeline assets, etc.) get a NULL lookup key and fall to the
--   UDF result. files_dim is restricted to distinct (wiki_db, file_name)
--   pairs that appear in this hour's upload_keys (upload_file_keys CTE) so
--   Spark does not read every file row for active wikis (e.g. all of
--   commonswiki for the snapshot) — that scan can OOM executors and surface
--   as RpcResponse / ClosedChannelException.
--
--   File-name decoding: parse_media_file_url already calls PercentDecoder.
--   decode once on uri_path. Some webrequest paths arrive doubly encoded,
--   so we apply PercentDecoder a second time via reflect to recover those
--   matches (literal '+' stays literal, stray '%' stays literal, valid
--   '%XX' is decoded).
--
-- Usage:
-- spark3-sql
--         --name=mediarequest_hourly_manual \
--         --master=yarn \
--         -f mediarequest/hourly.hql \
--         -d refinery_hive_shaded=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/refinery-hive-shaded.jar \
--         -d source_table=wmf.webrequest                                 \
--         -d destination_table=wmf.mediarequest                          \
--         -d mediawiki_file_table=wmf_raw.mediawiki_file                 \
--         -d mediawiki_filetypes_table=wmf_raw.mediawiki_filetypes       \
--         -d mw_snapshot=2021-01                                         \
--         -d coalesce_partitions=64                                      \
--         -d year=2021                                                   \
--         -d month=2                                                     \
--         -d day=9                                                       \
--         -d hour=6
--
SET parquet.compression = SNAPPY;

ADD JAR ${refinery_hive_shaded};
CREATE TEMPORARY FUNCTION parse_media_file_url AS 'org.wikimedia.analytics.refinery.hive.GetMediaFilePropertiesUDF';
CREATE TEMPORARY FUNCTION classify_referer AS 'org.wikimedia.analytics.refinery.hive.GetRefererTypeUDF';
CREATE TEMPORARY FUNCTION referer_wiki AS 'org.wikimedia.analytics.refinery.hive.GetRefererWikiUDF';

with upload_webrequests as (
    SELECT
        response_size,
        parse_media_file_url(uri_path) parsed_url,
        referer_wiki(referer) referer_wiki,
        classify_referer(referer) classified_referer,
        agent_type
    FROM ${source_table}
    WHERE webrequest_source='upload'
      AND year = ${year}
      AND month = ${month}
      AND day = ${day}
      AND hour = ${hour}
      AND uri_host = 'upload.wikimedia.org'
      AND ( http_status = 200 -- No 304 per RFC discussion
        OR ( http_status = 206
            AND SUBSTR(`range`, 1, 8) = 'bytes=0-'
            AND `range` != 'bytes=0-0' ))
),
upload_paths as (
    SELECT
        p.*,
        REPLACE(p.path_segments[2], '-', '_') AS normalized_site_key
    FROM (
        SELECT
            u.*,
            SPLIT(u.parsed_url.base_name, '/') AS path_segments
        FROM upload_webrequests u
        WHERE u.parsed_url.base_name IS NOT NULL
    ) p
),
-- Resolve each request to a (wiki_db, file_name) lookup key.
upload_keys as (
    SELECT
        u.response_size,
        u.parsed_url,
        u.referer_wiki,
        u.classified_referer,
        u.agent_type,
        CONCAT(u.normalized_site_key,
            CASE
                WHEN u.path_segments[1] = 'wikipedia' THEN 'wiki'
                WHEN u.path_segments[1] IN (
                    'wikimedia', 'wikibooks', 'wiktionary', 'wikiquote',
                    'wikisource', 'wikiversity', 'wikivoyage', 'wikinews'
                ) THEN u.path_segments[1]
                ELSE NULL
            END
        ) AS lookup_wiki_db,
        -- parse_media_file_url already runs PercentDecoder.decode on uri_path
        -- once. Some webrequest paths arrive doubly percent-encoded, so after one pass we still see
        -- residual '%XX'. Apply PercentDecoder a second time via reflect to
        -- collapse those, while still preserving literal '+' (URLDecoder would
        -- wrongly turn '+' into space — see e.g. files like
        -- 'JAH_0808_AM_36_HOURS_TO_HALLUCINATION_+_SO_HOW_LONG_DOES_IT_TAKE?.mpg').
        -- Empirically validated: zero rows in mediawiki_file.file_name match
        -- '%[0-9A-Fa-f]{2}', so a second pass cannot over-decode any real
        -- title.
        reflect(
            'org.wikimedia.analytics.refinery.core.PercentDecoder',
            'decode',
            ELEMENT_AT(u.path_segments, -1)
        ) AS lookup_file_name
    FROM upload_paths u
),
-- Distinct (wiki_db, file_name) keys seen this hour. Joining mediawiki_file
-- to this set limits the dimension to rows the join can ever hit.
upload_file_keys as (
    -- One row per (wiki_db, file_name); GROUP BY is equivalent to SELECT DISTINCT
    -- on both columns and makes the pair-wise dedupe obvious (DISTINCT is not
    -- "only on the first column" — it is always the full projected row).
    SELECT
        lookup_wiki_db   AS wiki_db,
        lookup_file_name AS file_name
    FROM upload_keys
    WHERE lookup_wiki_db IS NOT NULL
      AND lookup_file_name IS NOT NULL
    GROUP BY lookup_wiki_db, lookup_file_name
),
-- Get mediawiki_file + mediawiki_filetypes, at the previous-month snapshot, only
-- for (wiki_db, file_name) pairs in upload_file_keys. 
-- rows whose key is absent from mediawiki_file still get NULL db_class.
files_dim as (
    SELECT
        f.wiki_db,
        f.file_name,
        CASE ft.ft_media_type
            WHEN 'BITMAP'     THEN 'image'
            WHEN 'DRAWING'    THEN 'image'
            WHEN 'AUDIO'      THEN 'audio'
            WHEN 'VIDEO'      THEN 'video'
            WHEN 'MULTIMEDIA' THEN
                CASE
                    WHEN ft.ft_major_mime = 'audio' THEN 'audio'
                    WHEN ft.ft_major_mime = 'video' THEN 'video'
                    ELSE NULL
                END
            WHEN 'OFFICE'     THEN
                CASE
                    WHEN ft.ft_minor_mime IN ('pdf', 'vnd.djvu') THEN 'document'
                    ELSE 'other'
                END
            WHEN 'TEXT'       THEN
                CASE
                    WHEN ft.ft_minor_mime = 'plain' THEN 'document'
                    ELSE 'other'
                END
            WHEN '3D'         THEN 'other'
            WHEN 'ARCHIVE'    THEN 'other'
            WHEN 'EXECUTABLE' THEN 'other'
            ELSE NULL
        END AS db_media_classification
    FROM ${mediawiki_file_table} f
    JOIN ${mediawiki_filetypes_table} ft
        ON f.file_type = ft.ft_id
        AND f.snapshot = ft.snapshot
        AND f.wiki_db = ft.wiki_db
    INNER JOIN upload_file_keys k
        ON f.wiki_db = k.wiki_db
        AND f.file_name = k.file_name
    WHERE f.snapshot = '${mw_snapshot}'
      AND ft.snapshot = '${mw_snapshot}'
      AND f.file_deleted = 0
),
upload_enriched as (
    SELECT
        u.response_size,
        u.parsed_url,
        u.referer_wiki,
        u.classified_referer,
        u.agent_type,
        -- Agree-first override + fixed 'document': Extension based
        -- classification  wins when it says 'document' 
        -- (URL extension is authoritative for pdf/djvu/srt/txt).
        -- Otherwise only change when the DB has an opinion AND it disagrees
        -- with the URL-extension-based UDF result.
        CASE
            WHEN u.parsed_url.media_classification = 'document'
                THEN u.parsed_url.media_classification
            WHEN d.db_media_classification IS NULL
                THEN u.parsed_url.media_classification
            WHEN d.db_media_classification = u.parsed_url.media_classification
                THEN u.parsed_url.media_classification
            ELSE d.db_media_classification
        END AS media_classification
    FROM upload_keys u
    LEFT JOIN files_dim d
        ON u.lookup_wiki_db IS NOT NULL
        AND d.wiki_db = u.lookup_wiki_db
        AND d.file_name = u.lookup_file_name
)
INSERT OVERWRITE TABLE ${destination_table}
PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
SELECT /*+ COALESCE(${coalesce_partitions}) */
    parsed_url.base_name base_name,
    media_classification,
    parsed_url.file_type file_type,
    SUM(response_size) total_bytes,
    COUNT(*) request_count,
    parsed_url.transcoding transcoding,
    agent_type,
    IF(classified_referer = 'internal',
       COALESCE(referer_wiki, 'internal'),
       COALESCE(classified_referer, 'unknown')) referer,
    CONCAT(
            LPAD(${year}, 4, "0"), '-',
            LPAD(${month}, 2, "0"), '-',
            LPAD(${day}, 2, "0"), 'T',
            LPAD(${hour}, 2, "0"),
            ':00:00Z'
        ) dt
FROM upload_enriched
WHERE parsed_url.base_name IS NOT NULL
GROUP BY
    parsed_url.base_name,
    media_classification,
    parsed_url.file_type,
    IF(classified_referer = 'internal', COALESCE(referer_wiki, 'internal'), COALESCE(classified_referer, 'unknown')),
    parsed_url.transcoding,
    agent_type
;
