-- Create table statement for the commons_category_metrics table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_commons_media_file_metrics_snapshot.hql \
--                --database wmf_contributors
--                -d location=/wmf/data/wmf_contributors/commons/media_file_metrics_snapshot
--

CREATE EXTERNAL TABLE IF NOT EXISTS `commons_media_file_metrics_snapshot`
(
    `media_file`            STRING          COMMENT 'The name of the media file this row refers to. Coincides with the page title of the media file page in Commons. URL version (with underscores).',
    `media_type`            STRING          COMMENT 'The media type of the media file, coming from the Image table (img_media_type): BITMAP, VIDEO, etc.',
    `categories`            ARRAY<STRING>   COMMENT 'The category names that the media file is directly associated with.',
    `primary_categories`    ARRAY<STRING>   COMMENT 'The top ancestor category names of the media file. They should be in the Commons institution category allow-list. Ideally, there should be only one primary category, but since we can not control that from MediaWiki, we accept multiple primary categories.',
    `leveraging_wiki_count` BIGINT          COMMENT 'The number of wikis featuring this media file at least in one (namespace=0) page.',
    `leveraging_page_count` BIGINT          COMMENT 'The number of (namespace=0) pages featuring this media file across all wikis.',
    `month`                 STRING          COMMENT 'The month after the end of which we calculate the data (YYYY-MM). For example, if we are calculating the data after March 2024 (even if it’s i.e. April 4th) the value should be “2024-03”. This is so, to be consistent with the sibling incremental datasets (Pageviews by category, Pageviews by media file, and Edits).'
) USING ICEBERG
PARTITIONED BY (MONTH)
LOCATION '${location}'
;