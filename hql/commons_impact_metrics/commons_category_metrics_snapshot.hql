-- This query computes information about the GLAM Commons categories at the time the snapshot is taken.
-- It includes both primary categories and their subcategories, recursively down the tree to a given maximum level.
--
-- Parameters:
--     category_and_media_with_usage_map_table         -- Read data from here
--     mediawiki_page_table                            -- Read data from here
--     commons_category_metrics_snapshot_table         -- Insert results here
--     snapshot                                        -- YYYY-MM to compute for
--     coalesce_partitions                             -- Number of partitions to write
--
-- Usage:
--     spark3-sql -f commons_category_metrics_snapshot.hql \
--                -d category_and_media_with_usage_map_table=tmp.category_and_media_with_usage_map \
--                -d mediawiki_page_table=wmf_raw.mediawiki_page \
--                -d commons_category_metrics_snapshot_table=wmf_contributors.commons_category_metrics_snapshot \
--                -d snapshot=2024-02 \
--                -d coalesce_partitions=4

DELETE
FROM ${commons_category_metrics_snapshot_table}
WHERE month = '${snapshot}';

WITH category_id_title (
    SELECT
        page_id AS category_page_id,
        page_title AS category_title
    FROM ${mediawiki_page_table}
    WHERE snapshot = '${snapshot}'
      AND wiki_db = 'commonswiki'
),

category_primary_pairs (
    SELECT
        page_id AS category_page_id,
        explode(primary_categories) AS primary_category_page_id
    FROM ${category_and_media_with_usage_map_table}
    WHERE page_type != 'file'
),

categories_with_primaries (
    SELECT
        cpp.category_page_id,
        cit.category_title,
        collect_set(cpp.primary_category_page_id) AS primary_category_page_ids,
        collect_set(cit2.category_title) AS primary_category_titles
    FROM category_primary_pairs cpp
        INNER JOIN category_id_title cit
            ON cpp.category_page_id = cit.category_page_id
        INNER JOIN category_id_title cit2
            ON cpp.primary_category_page_id = cit2.category_page_id
    GROUP BY
        cpp.category_page_id,
        cit.category_title
),

files_with_primaries_parents_and_imagelinks (
    SELECT
        page_id AS media_file_page_id,
        primary_categories AS primary_category_page_ids,
        parent_categories AS parent_category_page_ids,
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

shallow_metrics (
    SELECT
        cwp.category_title,
        cwp.primary_category_titles,
        count(fpx.parent_category_page_id) AS media_file_count,
        sum(if(fpx.used, 1, 0)) AS used_media_file_count,
        cardinality(array_distinct(flatten(collect_list(fpx.wikis_where_used)))) AS leveraging_wiki_count,
        cardinality(array_distinct(flatten(collect_list(fpx.fqn_articles_where_used)))) AS leveraging_page_count
    FROM categories_with_primaries cwp
        LEFT JOIN media_files_with_imagelinks_parent_explode fpx
            ON (cwp.category_page_id == fpx.parent_category_page_id)
    GROUP BY
        cwp.category_title,
        cwp.primary_category_titles
),

deep_metrics (
    SELECT
        cwp.category_title,
        count(fpx.primary_category_page_id) AS media_file_count_deep,
        sum(if(fpx.used, 1, 0)) AS used_media_file_count_deep,
        cardinality(array_distinct(flatten(collect_list(fpx.wikis_where_used)))) AS leveraging_wiki_count_deep,
        cardinality(array_distinct(flatten(collect_list(fpx.fqn_articles_where_used)))) AS leveraging_page_count_deep
    FROM categories_with_primaries cwp
        LEFT JOIN media_files_with_imagelinks_primary_explode fpx
            ON (cwp.category_page_id == fpx.primary_category_page_id)
    GROUP BY cwp.category_title
)

INSERT
INTO ${commons_category_metrics_snapshot_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
    sh.category_title AS category,
    sh.primary_category_titles AS primary_categories,
    sh.media_file_count,
    dp.media_file_count_deep,
    sh.used_media_file_count,
    dp.used_media_file_count_deep,
    sh.leveraging_wiki_count,
    dp.leveraging_wiki_count_deep,
    sh.leveraging_page_count,
    dp.leveraging_page_count_deep,
    '${snapshot}' AS month
FROM shallow_metrics sh
    INNER JOIN deep_metrics dp
        ON (sh.category_title == dp.category_title)
;
