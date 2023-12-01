
with category_names (
 select page_id,
        page_title
   from wmf_raw.mediawiki_page p
  where snapshot = '2023-10'
    and wiki_db = 'commonswiki'
),
primary_categories (
 select /*+ BROADCAST(cam) */
        page_id,
        explode(array_distinct(transform(cam.category_paths, path -> path[cardinality(path) - 1]))) as primary_category_page_id
   from milimetric.category_and_media cam
  where page_type != 'file'
),
primary_categories_with_names (
 select category.page_id,
        category.page_title,
        collect_set(primary.page_title) as primary_category_names,
        collect_set(primary_category_page_id) as primary_category_ids
   from primary_categories pc
            inner join
        category_names category   on category.page_id = pc.page_id
            inner join
        category_names primary    on primary.page_id = primary_category_page_id
  group by category.page_id,
        category.page_title
),
files_with_ancestors_parents_and_imagelinks (
 select /*+ BROADCAST(cam) */
        cam.page_id,
        array_remove(array_distinct(flatten(cam.category_paths)), cam.page_id) as ancestors,
        array_distinct(transform(cam.category_paths, path -> path[1])) as parents,
        wum.usage_map,
        map_keys(wum.usage_map) as wikis_where_used,
        -- munge together wiki and imagelink article to allow deduplication later
        if(wum.usage_map is null, array(), flatten(transform(map_entries(wum.usage_map), x -> transform(map_keys(x.value), xi -> concat(x.key, xi))))) as fqn_articles_where_used,
        (wum.usage_map is not null and cardinality(flatten(transform(map_values(wum.usage_map), f -> map_keys(f)))) > 0) as used
   from milimetric.category_and_media cam
            left join
        mforns.category_and_media_with_usage_map wum  on cam.page_id = wum.page_id
  where cam.page_type = 'file'
)
-- with this cross join and without the broadcast, the table is read 200x (once on every partition I think)
-- and memory problems ensue, even though it's small... still some unknowns here, but broadcast works
 select /*+ BROADCAST(file) */
        category.page_title as name,
        category.primary_category_names as primary_categories,
        cardinality(collect_set(
          if(array_contains(file.parents, category.page_id), file.page_id, null)
        )) as media_files,
        cardinality(collect_set(
          if(array_contains(file.ancestors, category.page_id), file.page_id, null)
        )) as tree_media_files,
        cardinality(collect_set(
          if(array_contains(file.parents, category.page_id), if(file.used, file.page_id, null), null)
        )) as media_files_used,
        cardinality(collect_set(
          if(array_contains(file.ancestors, category.page_id), if(file.used, file.page_id, null), null)
        )) as tree_media_files_used,
        cardinality(array_distinct(flatten(collect_list(
          if(array_contains(file.parents, category.page_id), file.wikis_where_used, array())
        )))) as leveraging_wikis,
        cardinality(array_distinct(flatten(collect_list(
          if(array_contains(file.ancestors, category.page_id), file.wikis_where_used, array())
        )))) as tree_leveraging_wikis,
        cardinality(array_distinct(flatten(collect_list(
          if(array_contains(file.parents, category.page_id), file.fqn_articles_where_used, null)
        )))) as leveraging_articles,
        cardinality(array_distinct(flatten(collect_list(
          if(array_contains(file.ancestors, category.page_id), file.fqn_articles_where_used, null)
        )))) as tree_leveraging_articles,
        '2023-10' as snapshot
   from primary_categories_with_names category
            cross join
        files_with_ancestors_parents_and_imagelinks file
  group by category.page_title,
        category.primary_category_names
