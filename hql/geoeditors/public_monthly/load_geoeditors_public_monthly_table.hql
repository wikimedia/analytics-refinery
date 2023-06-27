-- Load geoeditors public monthly data into the given table.
--
-- This has fewer dimensions than the private dataset
-- and limits what data is output as follows:
--   * respects a disallowed list of countries
--   * only reports '5 to 99' and '100+' activity levels
--   * only reports numbers for Wikipedia projects
--   * excludes private or closed wikis
--   * excludes wikis with less than 3 active editors
--   * aggregates registered and anonymous editors[2]
--   * buckets output to obscure exact numbers (bucket size of 10)
--
-- [2] The final count of editors is an aggregate of both registered and
--     anonymous editors. It may happen that an editor edits as both registered
--     and anonymous in the same month. If so, that editor is going to be
--     counted twice.
--
-- Usage:
--     spark-sql -f load_geoeditors_public_monthly_table.hql \
--               -d editors_daily_table=wmf.editors_daily \
--               -d geoeditors_monthly_table=wmf.geoeditors_monthly \
--               -d country_info_table=canonical_data.countries \
--               -d disallowed_countries_table=canonical_data.countries \
--               -d project_namespace_map_table=wmf_raw.mediawiki_project_namespace_map \
--               -d destination_table=wmf.geoeditors_public_monthly \
--               -d month=2019-07                                   \
--               -d coalesce=1

WITH active_wikis AS (
    SELECT wiki_db
    FROM (
        SELECT
            wiki_db,
            user_fingerprint_or_name
        FROM ${editors_daily_table}
        WHERE
            month = '${month}' AND
            -- Spark 2 will not let us save [] so we handle both:
            -- size(NULL) returns -1 and size([]) returns 0
            size(user_is_bot_by) <= 0 AND
            action_type IN (0, 1)
        GROUP BY
            wiki_db,
            user_fingerprint_or_name
        HAVING sum(edit_count) >= 5
    ) AS active_editors
    GROUP BY wiki_db
    HAVING count(*) >= 3
),
public_active_wikipedias AS (
    SELECT
        wiki_db,
        substring(hostname, 1, length(hostname) - 4) AS project
    FROM active_wikis
        INNER JOIN ${project_namespace_map_table} ON
            dbname = wiki_db AND
            snapshot = '${month}' AND
            hostname LIKE '%.wikipedia.org' AND
            hostname NOT LIKE 'test%'
    GROUP BY
        wiki_db,
        hostname
),
ceil_counts AS (
    SELECT /*+ BROADCAST(disallowed_countries) */
        g.wiki_db,
        w.project,
        c.name AS country_name,
        g.country_code,
        activity_level,
        ceil(sum(distinct_editors) / 10) * 10 AS editors_ceil
    FROM ${geoeditors_monthly_table} AS g
        INNER JOIN public_active_wikipedias AS w ON w.wiki_db = g.wiki_db
        INNER JOIN ${country_info_table} AS c ON c.iso_code = g.country_code
        LEFT ANTI JOIN ${disallowed_countries_table} AS disallowed_countries
            ON disallowed_countries.iso_code = g.country_code
                AND disallowed_countries.is_protected IS TRUE
    WHERE
        month = '${month}' AND
        activity_level IN ('5 to 99', '100 or more')
    GROUP BY
        g.wiki_db,
        w.project,
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
    /*+ COALESCE(${coalesce}) */
        wiki_db,
        project,
        country_name,
        country_code,
        activity_level,
        editors_ceil
    FROM ceil_counts
    WHERE editors_ceil > 0
;
