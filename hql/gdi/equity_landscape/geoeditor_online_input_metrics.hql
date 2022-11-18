-- Aggregates data from geoeditors_monthly table, groups it accodring to the project bins and then joins it to the geoeditor_monthly table for active editors.
--
-- Parameters:
--     source_table         -- Read raw data from here
--     geoeditor_metrics    -- Read geoeditor_input_metrics from here
--     destination_table    -- Insert results here
--     year                 -- YYYY to compute statistics for
--
-- Usage:
--     hive -f geoeditor_online_input_metrics.hql                         \
--          -d source_table=wmf.geoeditors_monthly                        \
--          -d geoeditor_metrics=gdi.geoeditor_input_metrics              \
--          -d destination_table=gdi.geoeditor_online_input_metrics       \
--          -d year=2021
--
WITH average_active_editors AS (
SELECT country_code,
       SUM(distinct_editors) / count(distinct month) monthly_distinct_editors
  FROM ${source_table}
 WHERE month like '${year}-%'
   AND activity_level != '1 to 4'
 GROUP BY country_code
),
average_edits AS (
 SELECT country_code,
        sum(distinct_editors)/ count(distinct month) editorship_monthly
  FROM ${geoeditor_metrics}
 WHERE year = ${year}
 GROUP BY country_code
)

INSERT OVERWRITE TABLE ${destination_table} PARTITION (year='${year}')
SELECT act.country_code,
       act.monthly_distinct_editors,
       edit.editorship_monthly,
       IF(edit.editorship_monthly =0,0,act.monthly_distinct_editors/edit.editorship_monthly)  as percent_editors_active
  FROM average_active_editors act
  JOIN average_edits edit ON (act.country_code = edit.country_code);
