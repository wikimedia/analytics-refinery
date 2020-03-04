-- Query tsv geoeditors monthly data from the geoeditors monthly public table to be published as a flat file
--
-- This has fewer dimensions than the private dataset and limits what data is output as follows:
--  * respects a blacklist of countries
--  * only reports 5 to 99 and 100+ activity levels
--  * only reports numbers for Wikipedia projects or central projects like commons
--  * only considers wikis with 3 or more active editors
--  * buckets output to obscure exact numbers
--  * uses the mediawiki_project_namespace_map table to exclude private or closed wikis
--
-- See https://stats.wikimedia.org/EN/ProjectTrendsActiveWikis.html for rationale
-- on the 3 or more active editors decision, basically wikis don't really take off
-- unless they have at least a few people collaborating and vetting content
--
-- Usage:
--     hive -f generate_geoeditors_dump.hql \
--         -d source_table=wmf.geoeditors_public_monthly \
--         -d destination_directory=/tmp/druid_private/geoeditors_public_monthly \
--         -d month=2019-07
--
-- The schema of the file output:
-- `wiki_db`             string      COMMENT 'The wiki database the editors worked in',
-- `country`             string      COMMENT 'The country this group of editors geolocated to, including Unknown',
-- `activity_level`      string      COMMENT 'How many edits this group of editors performed, can be "5 to 99", or "100 or more"',
-- `editors_lower_bound` bigint      COMMENT 'At least this many editors at this activity level',
-- `editors_upper_bound` bigint      COMMENT 'At most this many editors at this activity level'

SET hive.exec.compress.output=false;

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    SELECT
        CONCAT_WS(
            "	",
            wiki_db,
            country_name,
            activity_level,
            CAST((editors_ceil - 9) AS string), -- editors lower bound
            CAST(editors_ceil AS string) -- editors upper bound
        ) tab_separated
    FROM ${source_table}
    WHERE month = '${month}'
;
