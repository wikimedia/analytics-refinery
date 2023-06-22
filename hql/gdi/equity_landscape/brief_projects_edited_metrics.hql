-- Extracts data from the various gdi tables and stores the metrics as overall engagement
--
-- Parameters:
--     source_table              -- Read raw data from here
--     country_data_table        -- Read country data from here
--     wiki_data_table           -- Read wiki data from here
--     destination_table         -- Insert results here
--     year                      -- YYYY to compute statistics for
--
-- Usage:
--     hive -f brief_projects_edited_metrics.hql                                 \
--          -d source_table=wmf.geoeditors_monthly                               \
--          -d wiki_data_table=canonical.wikis                                   \
--          -d country_data_table=gdi.country_meta_data                          \
--          -d destination_table=gdi.brief_projects_edited_metrics               \
--          -d year=2021
--

WITH
    monthly_avg AS (
        SELECT country_code,
               ROUND(AVG(distinct_editors)) AS average_monthly_acitve_editors,
               wiki_db
        FROM ${source_table}
       WHERE month like  '${year}-%'
       GROUP BY country_code, wiki_db
     ),
    monthly_sum AS (
        SELECT country_code,
               SUM(average_monthly_acitve_editors) AS total_editors
          FROM monthly_avg
         GROUP BY country_code
    ),
     projects_edited AS (
        SELECT monthly_avg.country_code,
               monthly_avg.average_monthly_acitve_editors,
               ROUND(monthly_avg.average_monthly_acitve_editors / monthly_sum.total_editors * 100, 2) AS proportion,
               monthly_avg.wiki_db
          FROM monthly_avg
          LEFT JOIN monthly_sum ON (monthly_avg.country_code = monthly_sum.country_code)
        ),
      brief_projects_edited AS (
        SELECT countries.country_code_iso_2                   AS country_code,
               countries.canonical_country_name               AS country_name,
               projects_edited.average_monthly_acitve_editors,
               projects_edited.proportion,
               projects_edited.wiki_db,
               wikis.english_name                             AS project_label,
               wikis.language_name                            AS language
          FROM projects_edited
          LEFT JOIN ${wiki_data_table} wikis ON (projects_edited.wiki_db = wikis.database_code)
          LEFT JOIN ${country_data_table} countries ON (projects_edited.country_code = countries.country_code_iso_2)
     )
INSERT OVERWRITE TABLE ${destination_table} PARTITION (year = '${year}')
SELECT country_code,
       country_name,
       average_monthly_acitve_editors,
       proportion,
       wiki_db,
       project_label,
       language
  FROM brief_projects_edited