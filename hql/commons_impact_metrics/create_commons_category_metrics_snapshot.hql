-- Create table statement for the commons_category_metrics table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_commons_category_metrics_snapshot.hql \
--                --database wmf_contributors \
--                -d location=/wmf/data/wmf_contributors/commons/category_metrics_snapshot
--

CREATE EXTERNAL TABLE IF NOT EXISTS `commons_category_metrics_snapshot`
(
    `category`                   STRING         COMMENT 'The name of the category this row refers to. Coincides with the page title of the category page in Commons. URL version (with underscores).',
    `parent_categories`          ARRAY<STRING>  COMMENT 'The immediate ancestor (parent) category names of this rows category.',
    `primary_categories`         ARRAY<STRING>  COMMENT 'The top ancestor category names of this rows category. They should be in the Commons institution category allow-list. Ideally, there should be only one primary category, but since we can not control that from MediaWiki, we accept multiple primary categories.',
    `media_file_count`           INT            COMMENT 'The number of media files contained in this (shallow) category.',
    `media_file_count_deep`      INT            COMMENT 'The number of media files contained in this (deep) category tree.',
    `used_media_file_count`      INT            COMMENT 'The number of media files from this (shallow) category featured in at least one wiki page.',
    `used_media_file_count_deep` INT            COMMENT 'The number of media files from this (deep) category tree featured in at least one wiki page.',
    `leveraging_wiki_count`      INT            COMMENT 'The number of wikis featuring at least one of this (shallow) category’s media files.',
    `leveraging_wiki_count_deep` INT            COMMENT 'The number of wikis featuring at least one of this (deep) category tree’s media files.',
    `leveraging_page_count`      INT            COMMENT 'The number of (namespace=0) pages featuring at least one of this (shallow) category’s media files.',
    `leveraging_page_count_deep` INT            COMMENT 'The number of (namespace=0) pages featuring at least one of this (deep) category tree’s media files.',
    `year_month`                 STRING         COMMENT 'The month after the end of which we calculate the data (YYYY-MM). For example, if we are calculating the data after March 2024 (even if it’s i.e. April 4th) the value should be “2024-03”. This is so, to be consistent with the sibling incremental datasets (Pageviews by category, Pageviews by media file, and Edits).'
) USING ICEBERG
PARTITIONED BY (year_month)
LOCATION '${location}'
;
