-- Aggregate geoeditors_daily data into edits by country and other dimensions
--
-- Parameters:
--     refinery_jar_version -- Version of the jar to import for UDFs
--     artifacts_directory  -- Where the UDF jars are
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     month                -- YYYY-MM to compute statistics for
--
-- Usage:
--     hive -f insert_geoeditors_edits_monthly_data.hql      \
--         -d source_table=wmf.geoeditors_daily              \
--         -d destination_table=wmf.geoeditors_edits_monthly \
--         -d month=2018-12
--

INSERT OVERWRITE TABLE ${destination_table}
       PARTITION (month='${month}')

     SELECT wiki_db,
            country_code,
            user_is_anonymous as edits_are_anonymous,
            sum(edit_count) as edit_count,
            sum(namespace_zero_edit_count) as namespace_zero_edit_count

       from ${source_table}
      where month = '${month}'

      group by wiki_db,
            country_code,
            user_is_anonymous
;
