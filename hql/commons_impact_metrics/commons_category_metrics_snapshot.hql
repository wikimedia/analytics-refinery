-- This query computes information about the GLAM Commons categories at the time the snapshot is taken.
-- It includes both primary categories and their subcategories, recursively down the tree to a given maximum level.
--
-- Parameters:
--     category_and_media_with_usage_map_table         -- Read data from here
--     commons_category_metrics_snapshot_table         -- Insert results here
--     year_month                                      -- YYYY-MM to compute for
--     coalesce_partitions                             -- Number of partitions to write
--
-- Usage:
--     spark3-sql -f commons_category_metrics_snapshot.hql \
--                -d category_and_media_with_usage_map_table=tmp.category_and_media_with_usage_map \
--                -d commons_category_metrics_snapshot_table=wmf_contributors.commons_category_metrics_snapshot \
--                -d year_month=2024-02 \
--                -d coalesce_partitions=4

DELETE
FROM ${commons_category_metrics_snapshot_table}
WHERE year_month = '${year_month}';

-- Get all categories with names and titles of their parents and primaries.
with categories_with_names_parents_and_primaries (
    SELECT
        page_id AS category_page_id,
        page_title AS category_title,
        map_values(parent_categories) AS parent_category_titles,
        map_values(primary_categories) AS primary_category_titles
    FROM ${category_and_media_with_usage_map_table}
    WHERE page_type != 'file'
),

-- Collect records about media file usage.
files_with_primaries_parents_and_imagelinks (
    SELECT
        page_id AS media_file_page_id,
        map_keys(primary_categories) AS primary_category_page_ids,
        map_keys(parent_categories) AS parent_category_page_ids,
        usage_map,
        map_keys(usage_map) AS wikis_where_used,
        -- munge together wiki and imagelink article to allow deduplication later
        -- <"wiki_1article_title_1","wiki_1article_title_2","...">
        if(usage_map IS NULL,
            array(),
            flatten(transform(
                map_entries(usage_map),
                x -> transform(map_keys(x.value), xi -> concat(x.key, xi))
            ))
        ) AS fqn_articles_where_used,
        (
            usage_map IS NOT NULL AND
            cardinality(flatten(transform(map_values(usage_map), f -> map_keys(f)))) > 0
        ) AS used
    FROM ${category_and_media_with_usage_map_table}
    WHERE page_type = 'file'
),

-- Explode media file data by parent categories to calculate shallow metrics.
media_files_with_imagelinks_parent_explode (
    SELECT
        media_file_page_id,
        explode(parent_category_page_ids) AS parent_category_page_id,
        usage_map,
        wikis_where_used,
        fqn_articles_where_used,
        used
    FROM files_with_primaries_parents_and_imagelinks
),

-- Explode media file data by primary categories to calculate deep metrics.
media_files_with_imagelinks_primary_explode (
    SELECT
        media_file_page_id,
        explode(primary_category_page_ids) AS primary_category_page_id,
        usage_map,
        wikis_where_used,
        fqn_articles_where_used,
        used
    FROM files_with_primaries_parents_and_imagelinks
),

-- Agregate shallow metrics.
shallow_metrics (
    SELECT
        cpp.category_title,
        cpp.parent_category_titles,
        cpp.primary_category_titles,
        count(fpx.parent_category_page_id) AS media_file_count,
        sum(if(fpx.used, 1, 0)) AS used_media_file_count,
        cardinality(array_distinct(flatten(collect_list(fpx.wikis_where_used)))) AS leveraging_wiki_count,
        cardinality(array_distinct(flatten(collect_list(fpx.fqn_articles_where_used)))) AS leveraging_page_count
    FROM categories_with_names_parents_and_primaries cpp
        LEFT JOIN media_files_with_imagelinks_parent_explode fpx
            ON (cpp.category_page_id == fpx.parent_category_page_id)
    GROUP BY
        cpp.category_title,
        cpp.parent_category_titles,
        cpp.primary_category_titles
),

-- Aggregate deep metrics.
deep_metrics (
    SELECT
        cpp.category_title,
        count(fpx.primary_category_page_id) AS media_file_count_deep,
        sum(if(fpx.used, 1, 0)) AS used_media_file_count_deep,
        cardinality(array_distinct(flatten(collect_list(fpx.wikis_where_used)))) AS leveraging_wiki_count_deep,
        cardinality(array_distinct(flatten(collect_list(fpx.fqn_articles_where_used)))) AS leveraging_page_count_deep
    FROM categories_with_names_parents_and_primaries cpp
        LEFT JOIN media_files_with_imagelinks_primary_explode fpx
            ON (cpp.category_page_id == fpx.primary_category_page_id)
    GROUP BY cpp.category_title
)

-- Write the data in the output format.
INSERT
INTO ${commons_category_metrics_snapshot_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
    sh.category_title AS category,
    sh.parent_category_titles AS parent_categories,
    sh.primary_category_titles AS primary_categories,
    sh.media_file_count,
    dp.media_file_count_deep,
    sh.used_media_file_count,
    dp.used_media_file_count_deep,
    sh.leveraging_wiki_count,
    dp.leveraging_wiki_count_deep,
    sh.leveraging_page_count,
    dp.leveraging_page_count_deep,
    '${year_month}' AS year_month
FROM shallow_metrics sh
    INNER JOIN deep_metrics dp
        ON (sh.category_title == dp.category_title)
;
