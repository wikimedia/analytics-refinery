-- This script generates a dataset that gives information about the GLAM Commons media files at the time
-- the snapshot is taken. It includes category information so that the user can filter by category.
--
-- Parameters:
--     category_and_media_with_usage_map_table         -- Read data from here
--     mediawiki_page_table                            -- Read data from here
--     mediawiki_image_table                           -- Read data from here
--     commons_media_file_metrics_snapshot_table       -- Insert results here
--     snapshot                                        -- YYYY-MM to compute for
--     coalesce_partitions                             -- Number of partitions to write
--
-- Usage:
--     spark3-sql -f commons_media_file_metrics_snapshot.hql                                           \
--                -d category_and_media_with_usage_map_table=tmp.category_and_media_with_usage_map       \
--                -d mediawiki_page_table=wmf_raw.mediawiki_page       \
--                -d mediawiki_image_table=wmf_raw.mediawiki_image       \
--                -d commons_media_file_metrics_snapshot_table=wmf_contributors.commons_media_file_metrics_snapshot     \
--                -d snapshot=2024-02 \
--                -d coalesce_partitions=4

DELETE
FROM ${commons_media_file_metrics_snapshot_table}
WHERE month = '${snapshot}';

WITH primary_and_direct_categories (
    SELECT
        page_id,
        direct_category_page_id,
        explode(primary_categories) AS primary_category_page_id
    FROM (
        SELECT
            page_id,
            explode(parent_categories) AS direct_category_page_id,
            primary_categories
        FROM ${category_and_media_with_usage_map_table}
        WHERE page_type = 'file'
    ) AS explode_parents
),

distinct_page_id (
    SELECT page_id FROM primary_and_direct_categories
    UNION
    SELECT primary_category_page_id FROM primary_and_direct_categories
    UNION
    SELECT direct_category_page_id FROM primary_and_direct_categories
),

page_lookup (
 SELECT d.page_id,
        page_title
   FROM distinct_page_id d
            INNER JOIN ${mediawiki_page_table} p
   ON d.page_id = p.page_id
   AND snapshot = '${snapshot}'
   AND wiki_db = 'commonswiki'
),

with_category_names (
     SELECT file.page_id,
         file.page_title AS name,
         collect_set(PRIMARY.page_title) AS primary_categories,
         collect_set(direct.page_title) AS direct_categories
     FROM primary_and_direct_categories
         INNER JOIN
         page_lookup file
     ON file.page_id = primary_and_direct_categories.page_id
         INNER JOIN
         page_lookup PRIMARY ON PRIMARY.page_id = primary_category_page_id
         INNER JOIN
         page_lookup direct ON direct.page_id = direct_category_page_id
     GROUP BY file.page_id,
         file.page_title
)

INSERT
INTO ${commons_media_file_metrics_snapshot_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
        name AS media_file,
        coalesce(mfi.img_media_type, 'image-renamed') AS media_type,
        direct_categories AS categories,
        wcm.primary_categories,
        IF (wum.usage_map IS NULL, 0, cardinality(map_keys(wum.usage_map))) AS leveraging_wiki_count,
        IF (wum.usage_map IS NULL, 0, cardinality(flatten(TRANSFORM (map_values(wum.usage_map), f -> map_keys(f))))) AS leveraging_page_count,
        '${snapshot}' AS MONTH
FROM with_category_names wcm
LEFT JOIN ${category_and_media_with_usage_map_table} wum
    ON wcm.page_id = wum.page_id
LEFT JOIN
${mediawiki_image_table} mfi
    ON name = img_name
AND mfi.wiki_db = 'commonswiki'
AND mfi.snapshot = '${snapshot}'
;
