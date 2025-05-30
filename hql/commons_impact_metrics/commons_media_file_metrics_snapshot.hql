-- This script generates a dataset that gives information about the GLAM Commons media files at the time
-- the snapshot is taken. It includes category information so that the user can filter by category.
--
-- Parameters:
--     category_and_media_with_usage_map_table         -- Read data from here
--     mediawiki_file_table                            -- Read data from here
--     mediawiki_filetypes_table                       -- Read data from here
--     commons_media_file_metrics_snapshot_table       -- Insert results here
--     year_month                                      -- YYYY-MM to compute for
--     coalesce_partitions                             -- Number of partitions to write
--
-- Usage:
--     spark3-sql -f commons_media_file_metrics_snapshot.hql \
--                -d category_and_media_with_usage_map_table=tmp.category_and_media_with_usage_map \
--                -d mediawiki_file_table=wmf_raw.mediawiki_file \
--                -d mediawiki_filetypes_table=wmf_raw.mediawiki_filetypes \
--                -d commons_media_file_metrics_snapshot_table=wmf_contributors.commons_media_file_metrics_snapshot \
--                -d year_month=2024-02 \
--                -d coalesce_partitions=4

DELETE
FROM ${commons_media_file_metrics_snapshot_table}
WHERE year_month = '${year_month}';

WITH files_with_media_types AS (
    SELECT
        f.file_name,
        coalesce(t.ft_media_type, 'image-renamed') AS media_type
    FROM ${mediawiki_file_table} f
    LEFT JOIN ${mediawiki_filetypes_table} t ON (f.file_type = t.ft_id)
    WHERE
        f.snapshot = '${year_month}' AND
        f.wiki_db = 'commonswiki' AND
        t.snapshot = '${year_month}' AND
        t.wiki_db = 'commonswiki'
)

INSERT INTO ${commons_media_file_metrics_snapshot_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
    cmu.page_title as media_file,
    fmt.media_type,
    map_values(cmu.parent_categories) AS categories,
    map_values(cmu.primary_categories) AS primary_categories,
    IF (cmu.usage_map IS NULL, 0, cardinality(map_keys(cmu.usage_map))) AS leveraging_wiki_count,
    IF (cmu.usage_map IS NULL, 0, cardinality(flatten(TRANSFORM (map_values(cmu.usage_map), f -> map_keys(f))))) AS leveraging_page_count,
    '${year_month}' AS year_month
FROM ${category_and_media_with_usage_map_table} cmu
LEFT JOIN files_with_media_types fmt ON (cmu.page_title = fmt.file_name)
WHERE
    cmu.page_type = 'file' AND
    cmu.usage_map IS NOT NULL
;
