-- Create table statement for the commons_pageviews_by_media_file table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_commons_pageviews_by_media_file.hql \
--                --database wmf_contributors
--                -d location=/wmf/data/wmf_contributors/commons/pageviews_by_media_file
--
CREATE EXTERNAL TABLE IF NOT EXISTS `commons_pageviews_by_media_file`
(
    `media_file`         STRING          COMMENT 'The name of the media file this row refers to. Coincides with the page title of the media file page in Commons. URL version (with underscores).',
    `categories`         ARRAY<STRING>   COMMENT 'The category names that the media file is directly associated with.',
    `primary_categories` ARRAY<STRING>   COMMENT 'The top ancestor category names of the media file. They should be in the Commons institution category allow-list. Ideally, there should be only one primary category, but since we can not control that from MediaWiki, we accept multiple primary categories.',
    `wiki`               STRING          COMMENT 'The canonical name of the visualized wiki, i.e.: “en.wikipedia” or “fr.wiktionary”. Only wikis that feature the media file at least once will appear here.',
    `page_title`         STRING          COMMENT 'The title of the visualized (namespace=0) page. URL version (with underscores). Only (namespace=0) pages featuring the media file will appear here.',
    `pageview_count`     BIGINT          COMMENT 'Aggregated pageview count for (namespace=0) pages featuring the media file. Rows with pageview_count=0 should be omitted!',
    `month`              STRING          COMMENT 'The month for which we aggregate the data (YYYY-MM).'
) USING ICEBERG
PARTITIONED BY (MONTH)
LOCATION '${location}'
;