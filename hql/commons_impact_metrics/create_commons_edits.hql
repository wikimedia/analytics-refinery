-- Create table statement for the commons_edits table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_commons_edits.hql \
--                --database wmf_contributors \
--                -d location=/wmf/data/wmf_contributors/commons/edits
--
CREATE EXTERNAL TABLE IF NOT EXISTS `commons_edits`
(
    `user_name`          STRING         COMMENT 'The user name of the user who performed the edit. This is resolved from the actor table’s actor_name. If no actor is found, it is set to ‘anonymous’. If it has been suppressed, it is set to ‘redacted’.',
    `edit_type`          STRING         COMMENT 'Either “create” (for the first revision of a media file page), or “update” (for all other revisions of the media file page).',
    `media_file`         STRING         COMMENT 'The name of the edited media file. Coincides with the page title of the media file page in Commons. URL version (with underscores).',
    `categories`         ARRAY<STRING>  COMMENT 'The category names that the media file is directly associated with.',
    `primary_categories` ARRAY<STRING>  COMMENT 'The top ancestor category names of the media file. They should be in the Commons institution category allow-list. Ideally, there should be only one primary category, but since we can not control that from MediaWiki, we accept multiple primary categories.',
    `dt`                 TIMESTAMP      COMMENT 'The timestamp of the edit.'
) USING ICEBERG
PARTITIONED BY (MONTHS(dt))
LOCATION '${location}'
;
