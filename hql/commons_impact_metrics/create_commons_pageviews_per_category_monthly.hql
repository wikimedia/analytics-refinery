-- Create table statement for the commons_pageviews_per_category_monthly table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_commons_pageviews_per_category_monthly.hql \
--                --database wmf_contributors
--                -d location=/wmf/data/wmf_contributors/commons/pageviews_per_category_monthly
--
CREATE EXTERNAL TABLE IF NOT EXISTS `commons_pageviews_per_category_monthly`
(
    `category`           STRING         COMMENT 'The name of the category this row refers to. Coincides with the page title of the category page in Commons. URL version (with underscores).',
    `category_scope`     STRING         COMMENT 'Either “shallow” (meaning only media files directly associated with the category were used to aggregate pageviews) or “deep” (meaning all media files within the category and all its recursive subcategories were used to aggregate pageviews).',
    `primary_categories` ARRAY<STRING>  COMMENT 'The top ancestor category names of the media file. They should be in the Commons institution category allow-list. Ideally, there should be only one primary category, but since we can not control that from MediaWiki, we accept multiple primary categories.',
    `wiki`               STRING         COMMENT 'The canonical name of the visualized wiki, i.e.: “en.wikipedia” or “fr.wiktionary”. Only wikis that feature at least one media file of the corresponding category will appear here.',
    `page_title`         STRING         COMMENT 'The title of the visualized (namespace=0) page. URL version (with underscores). Only (namespace=0) pages featuring at least one media file of the corresponding category will appear here.',
    `pageview_count`     BIGINT         COMMENT 'Aggregated pageview count for (namespace=0) pages featuring at least one media file from the category/scope. Rows with pageview_count=0 should be omitted!',
    `year_month`         STRING         COMMENT 'The month for which we aggregate the data (YYYY-MM).'
) USING ICEBERG
PARTITIONED BY (year_month)
LOCATION '${location}'
;
