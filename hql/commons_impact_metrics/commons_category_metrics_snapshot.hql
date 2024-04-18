-- This query computes information about the GLAM Commons categories at the time the snapshot is taken.
-- It includes both primary categories and all their subcategories, recursively down the tree.
--
-- Parameters:
--     category_and_media_with_usage_map_table         -- Read data from here
--     mediawiki_page_table                            -- Read data from here
--     commons_category_metrics_snapshot_table         -- Insert results here
--     snapshot                                        -- YYYY-MM to compute for
--     coalesce_partitions                             -- Number of partitions to write
--
-- Usage:
--     spark3-sql -f commons_category_metrics_snapshot.hql                                           \
--                -d category_and_media_with_usage_map_table=tmp.category_and_media_with_usage_map       \
--                -d mediawiki_page_table=wmf_raw.mediawiki_page       \
--                -d commons_category_metrics_snapshot_table=wmf_contributors.commons_category_metrics_snapshot     \
--                -d snapshot=2024-02 \
--                -d coalesce_partitions=4

DELETE
FROM ${commons_category_metrics_snapshot_table}
WHERE month = '${snapshot}';

WITH category_names (
    SELECT page_id,
           page_title
    FROM ${mediawiki_page_table} p
    WHERE snapshot = '${snapshot}'
      AND wiki_db = 'commonswiki'
),

primary_categories (
    SELECT page_id,
           explode(primary_categories) AS primary_category_page_id
    FROM ${category_and_media_with_usage_map_table}
    WHERE page_type != 'file'
),

primary_categories_with_names (
     SELECT category.page_id,
         category.page_title,
         collect_set(PRIMARY.page_title) AS primary_category_names,
         collect_set(primary_category_page_id) AS primary_category_ids
     FROM primary_categories pc
         INNER JOIN
         category_names category
     ON category.page_id = pc.page_id
         INNER JOIN
         category_names PRIMARY ON PRIMARY.page_id = primary_category_page_id
     GROUP BY category.page_id,
         category.page_title
),

files_with_ancestors_parents_and_imagelinks (
     SELECT
         page_id,
         ancestor_categories AS ancestors,
         parent_categories AS parents,
         usage_map,
         map_keys(usage_map) AS wikis_where_used,
         -- munge together wiki and imagelink article to allow deduplication later
         IF (usage_map IS NULL, ARRAY (), flatten(TRANSFORM (map_entries(usage_map), x -> TRANSFORM (map_keys(x.value), xi -> concat(x.key, xi))))) AS fqn_articles_where_used,
         -- <"nombre_wiki_1nombre_articulo_1","nombre_wiki_1nombre_articulo_2","...">
         (usage_map IS NOT NULL AND cardinality(flatten(TRANSFORM (map_values(usage_map), f -> map_keys(f)))) > 0) AS used
     FROM ${category_and_media_with_usage_map_table}
     WHERE page_type = 'file'
),

files_with_ancestors_parents_and_imagelinks_immediate_parent_explode (
     SELECT
         page_id,
         explode(parents) AS parent_page_id,
         usage_map,
         wikis_where_used,
         fqn_articles_where_used,
         used
     FROM files_with_ancestors_parents_and_imagelinks
),

files_with_ancestors_parents_and_imagelinks_ancestor_explode (
     SELECT
         page_id,
         explode(ancestors) AS ancestor_page_id,
         usage_map,
         wikis_where_used,
         fqn_articles_where_used,
         used
     FROM files_with_ancestors_parents_and_imagelinks
),

immediate_parent_metrics (
     SELECT
         category.page_title AS name,
         category.primary_category_names AS primary_categories,
         count(1) AS media_files,
         sum(IF (file.used, 1, 0)) AS media_files_used,
         cardinality(array_distinct(flatten(collect_list(file.wikis_where_used)))) AS leveraging_wikis,
         cardinality(array_distinct(flatten(collect_list(file.fqn_articles_where_used)))) AS leveraging_articles,
         '${snapshot}' AS snapshot
     FROM primary_categories_with_names category
         LEFT JOIN
         files_with_ancestors_parents_and_imagelinks_immediate_parent_explode file
     ON (category.page_id == file.parent_page_id)
     GROUP BY category.page_title,
         category.primary_category_names
),

ancestor_metrics (
     SELECT
         category.page_title AS name,
         category.primary_category_names AS primary_categories,
         count(1) AS tree_media_files,
         sum(IF (file.used, 1, 0)) AS tree_media_files_used,
         cardinality(array_distinct(flatten(collect_list(file.wikis_where_used)))) AS tree_leveraging_wikis,
         cardinality(array_distinct(flatten(collect_list(file.fqn_articles_where_used)))) AS tree_leveraging_articles,
         '${snapshot}' AS snapshot
     FROM primary_categories_with_names category
         LEFT JOIN
         files_with_ancestors_parents_and_imagelinks_ancestor_explode file
     ON (category.page_id == file.ancestor_page_id)
     GROUP BY category.page_title,
         category.primary_category_names
)

INSERT
INTO ${commons_category_metrics_snapshot_table}
SELECT /*+ COALESCE(${coalesce_partitions}) */
        im.name AS category,
        im.primary_categories,
        im.media_files AS media_file_count,
        an.tree_media_files AS media_file_count_deep,
        im.media_files_used AS used_media_file_count,
        an.tree_media_files_used AS used_media_file_count_deep,
        im.leveraging_wikis AS leveraging_wiki_count,
        an.tree_leveraging_wikis AS leveraging_wiki_count_deep,
        im.leveraging_articles AS leveraging_page_count,
        an.tree_leveraging_articles AS leveraging_page_count_deep,
        an.snapshot AS MONTH
FROM immediate_parent_metrics im
    INNER JOIN ancestor_metrics an
ON (im.name == an.name)
;
