WITH page_ids_with_categories AS (
    SELECT file_page_id,
           path[path_length - 1] AS primary_category_page_id,
           IF(path_length > 1, path[1], NULL) as category_page_id
    FROM (
        SELECT file_page_id,
               path,
               size(path) as path_length
        FROM (
            SELECT page_id AS file_page_id, explode(category_paths) as path
            FROM milimetric.category_and_media
            WHERE page_type = 'file'
        )
    )
),

mediawiki_page AS (
    SELECT *
    FROM wmf_raw.mediawiki_page
    WHERE wiki_db = 'commonswiki'
      AND snapshot = '2023-10'
),

mediawiki_revision AS (
    SELECT *
    FROM wmf_raw.mediawiki_revision
    WHERE wiki_db = 'commonswiki'
      AND snapshot = '2023-10'
),

mediawiki_actor AS (
    SELECT *
    FROM wmf_raw.mediawiki_private_actor
    WHERE wiki_db = 'commonswiki'
      AND snapshot = '2023-10'
)

SELECT pids_and_page_titles_and_edit_info.file_page_id,
       pids_and_page_titles_and_edit_info.primary_category_page_ids,
       pids_and_page_titles_and_edit_info.category_page_ids,
       pids_and_page_titles_and_edit_info.file_page_title,
       pids_and_page_titles_and_edit_info.primary_category_page_titles,
       pids_and_page_titles_and_edit_info.category_page_titles,
       pids_and_page_titles_and_edit_info.edit_timestamp,
       pids_and_page_titles_and_edit_info.edit_type,
       IF(is_edit_actor_visible AND mwa.actor_user IS NOT NULL, mwa.actor_name, 'anonymous') as edit_user
FROM (
    SELECT pids_and_page_titles.*,
           mwr.rev_actor as edit_actor,
           mwr.rev_deleted & 2 = 0 as is_edit_actor_visible,
           to_timestamp(mwr.rev_timestamp, 'yyyyMMddkkmmss') as edit_timestamp,
           IF(mwr.rev_parent_id == 0, 'create', 'update') as edit_type -- we want delete as well but need to figure
                                                                       -- how to get that one separately
    FROM (
        SELECT file_page_id,
               collect_set(primary_category_page_id) as primary_category_page_ids,
               collect_set(category_page_id) as category_page_ids,
               first(file_page_title) as file_page_title,
               collect_set(primary_category_page_title) as primary_category_page_titles,
               collect_set(category_page_title) as category_page_titles
        FROM (
            SELECT pids.*,
                   mwp1.page_title as file_page_title,
                   mwp2.page_title as primary_category_page_title,
                   mwp3.page_title as category_page_title
            FROM page_ids_with_categories pids
            INNER JOIN mediawiki_page mwp1 ON pids.file_page_id = mwp1.page_id
            INNER JOIN mediawiki_page mwp2 ON pids.primary_category_page_id = mwp2.page_id
            INNER JOIN mediawiki_page mwp3 ON pids.category_page_id = mwp3.page_id
        )
        GROUP BY file_page_id
    ) pids_and_page_titles
    INNER JOIN mediawiki_revision mwr ON pids_and_page_titles.file_page_id = mwr.rev_page
) pids_and_page_titles_and_edit_info
INNER JOIN mediawiki_actor mwa ON edit_actor = mwa.actor_id
