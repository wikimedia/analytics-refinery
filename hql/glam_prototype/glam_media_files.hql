with primary_and_direct_categories (
 select page_id,
        explode(array_distinct(transform(cam.category_paths, path -> path[cardinality(path) - 1]))) as primary_category_page_id,
        explode(array_distinct(transform(cam.category_paths, path -> path[1]))) as direct_category_page_id
   from milimetric.category_and_media cam
  where page_type = 'file'
),
distinct_page_id (
 select page_id from primary_and_direct_categories
 union
 select primary_category_page_id from primary_and_direct_categories
 union
 select direct_category_page_id from primary_and_direct_categories
),
page_lookup (
 select d.page_id,
        page_title
   from distinct_page_id d
            inner join
        wmf_raw.mediawiki_page p  on d.page_id = p.page_id
                                  and snapshot = '2023-10'
                                  and wiki_db = 'commonswiki'
),
-- note: I thought /*+ BROADCAST(page_lookup) */ would help here, but
-- it seems not to find page_lookup, maybe it doesn't work with CTEs?
-- also, it seems unnecessary because for the sizes we're doing this query
-- for it seems to broadcast the whole of category_and_media
with_category_names (
 select file.page_id,
        file.page_title as name,
        collect_set(primary.page_title) as primary_categories,
        collect_set(direct.page_title) as direct_categories
   from primary_and_direct_categories
            inner join
        page_lookup file                  on file.page_id = primary_and_direct_categories.page_id
            inner join
        page_lookup primary               on primary.page_id = primary_category_page_id
            inner join
        page_lookup direct                on direct.page_id = direct_category_page_id
  group by file.page_id,
        file.page_title
)
 select name,
        coalesce(mfi.img_media_type, 'image-renamed') as media_type,
        primary_categories,
        direct_categories as categories,
        if(wum.usage_map is null, 0, cardinality(map_keys(wum.usage_map))) as leveraging_wikis,
        if(wum.usage_map is null, 0, cardinality(flatten(transform(map_values(wum.usage_map), f -> map_keys(f))))) as leveraging_articles,
        '2023-10' as snapshot
   from with_category_names wcm
            left join
        mforns.category_and_media_with_usage_map wum    on wcm.page_id = wum.page_id
            left join
        wmf_raw.mediawiki_image mfi                     on name = img_name
                                                        and mfi.wiki_db = 'commonswiki'
                                                        and mfi.snapshot = '2023-10'
