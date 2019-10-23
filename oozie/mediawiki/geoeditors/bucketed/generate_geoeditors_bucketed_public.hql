-- Generate bucketed tsv geoeditors monthly data to be published as a flat file
-- This has fewer dimensions than the private dataset and limits what data is output as follows:
--
--  * respects a blacklist of countries
--  * only reports 5 to 99 and 100+ activity levels
--  * only reports numbers for Wikipedia projects or central projects like commons
--  * only considers wikis with 3 or more active editors
--  * buckets output to obscure exact numbers
--
-- See https://stats.wikimedia.org/EN/ProjectTrendsActiveWikis.html for rationale
-- on the 3 or more active editors decision, basically wikis don't really take off
-- unless they have at least a few people collaborating and vetting content
--
-- Usage:
--     hive -f generate_geoeditors_bucketed_public.hql \
--         -d source_table=wmf.geoeditors_monthly \
--         -d country_info_table=canonical_data.countries \
--         -d blacklist_country_table=wmf_raw.geoeditors_blacklist_country \
--         -d destination_directory=/tmp/druid_private/geoeditors_monthly_public \
--         -d month=2019-07
--
-- The schema of the file output:

-- `wiki_db`             string      COMMENT 'The wiki database the editors worked in',
-- `country`             string      COMMENT 'The country this group of editors geolocated to, including Unknown',
-- `activity_level`      string      COMMENT 'How many edits this group of editors performed, can be "5 to 99", or "100 or more"',
-- `editors_lower_bound` bigint      COMMENT 'At least this many editors at this activity level',
-- `editors_upper_bound` bigint      COMMENT 'At most this many editors at this activity level'

SET hive.exec.compress.output=false;

WITH active_wiki as (

 SELECT wiki_db
   FROM ${source_table}
  WHERE month = '${month}'
    AND wiki_db like '%wiki'
    AND wiki_db not like 'wikimania%'
    AND wiki_db not like 'test%'
    AND activity_level in ('5 to 99', '100 or more')
  GROUP BY wiki_db
 HAVING SUM(distinct_editors) > 2
) ,

exact_counts as (
 SELECT g.wiki_db,
        c.name as country,
        activity_level,
        SUM(distinct_editors) as distinct_editors

   FROM ${source_table} g
            INNER JOIN
        active_wiki w                   on w.wiki_db = g.wiki_db
            INNER JOIN
        ${country_info_table} c         on c.iso_code = g.country_code
            LEFT JOIN
        ${blacklist_country_table} b    on b.country_code = g.country_code

  WHERE month = '${month}'
    AND activity_level in ('5 to 99', '100 or more')
    AND b.country_code is null

  GROUP BY g.wiki_db,
        c.name,
        activity_level

  ORDER BY wiki_db,
        country,
        activity_level
  LIMIT 100000
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT
        CONCAT_WS("	",
            wiki_db,
            country,
            activity_level,
            cast(floor(distinct_editors / 10) * 10          as string), -- editors lower bound
            cast(ceil((distinct_editors + 1) / 10) * 10 - 1 as string)  -- editors upper bound

        ) tab_separated
   FROM exact_counts
;
