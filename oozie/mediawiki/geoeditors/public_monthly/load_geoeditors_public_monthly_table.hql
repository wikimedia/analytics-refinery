-- Load geoeditors monthly public data into a table
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
--     hive -f load_geoeditors_public_monthly_table.hql \
--         -d source_table=wmf.geoeditors_monthly \
--         -d country_info_table=canonical_data.countries \
--         -d blacklist_country_table=wmf.geoeditors_blacklist_country \
--         -d project_namespace_map_table=wmf_raw.mediawiki_project_namespace_map \
--         -d destination_table=wmf.geoeditors_public_monthly \
--         -d month=2019-07
--
-- The schema of the destination table:
-- `wiki_db`         string      COMMENT 'The wiki database the editors worked in',
-- `project_name`    string      COMMENT 'The project the editors worked in',
-- `country_name`    string      COMMENT 'The country this group of editors geolocated to, including Unknown',
-- `country_code`    string      COMMENT 'The ISO 3166-1 alpha-2 country code of the country this group of editors geolocated to, including Unknown as --',
-- `activity_level`  string      COMMENT 'How many edits this group of editors performed, can be "5 to 99", or "100 or more"',
-- `editors_ceil`    bigint      COMMENT 'At most this many editors at this activity level'
-- Partition information:
-- `month`           string      COMMENT 'The month that the data applies to'

SET hive.exec.compress.output=false;

WITH
active_wiki as (
    SELECT
        wiki_db,
        hostname
    FROM ${source_table}
        INNER JOIN
            ${project_namespace_map_table} ON
                dbname = wiki_db
                AND snapshot = '${month}'
                AND hostname LIKE '%.wikipedia.org'
                AND hostname NOT LIKE 'test%'
    WHERE
        month = '${month}'
        AND activity_level IN ('5 to 99', '100 or more')
    GROUP BY wiki_db, hostname
    HAVING SUM(distinct_editors) > 2
),
exact_counts as (
    SELECT
        g.wiki_db,
        w.hostname AS project_name,
        c.name AS country_name,
        g.country_code,
        activity_level,
        SUM(distinct_editors) as distinct_editors
    FROM
        ${source_table} AS g
            INNER JOIN active_wiki AS w ON w.wiki_db = g.wiki_db
            INNER JOIN ${country_info_table} AS c ON c.iso_code = g.country_code
            LEFT JOIN ${blacklist_country_table} AS b ON b.country_code = g.country_code
    WHERE
        month = '${month}'
        AND activity_level IN ('5 to 99', '100 or more')
        AND b.country_code IS null
    GROUP BY
        g.wiki_db,
        w.hostname,
        c.name,
        g.country_code,
        activity_level
    ORDER BY
        wiki_db,
        country_name,
        activity_level
    LIMIT 100000
)
INSERT OVERWRITE TABLE ${destination_table} PARTITION (month='${month}')
    SELECT
        wiki_db,
        project_name,
        country_name,
        country_code,
        activity_level,
        ceil(distinct_editors / 10) * 10  -- editors upper bound
    FROM exact_counts
    WHERE distinct_editors > 0
;
