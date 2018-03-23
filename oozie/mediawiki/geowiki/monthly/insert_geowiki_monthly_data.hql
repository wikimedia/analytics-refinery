-- Aggregate geowiki_daily data into the traditional kind of data kept by geowiki
--
-- Parameters:
--     refinery_jar_version -- Version of the jar to import for UDFs
--     artifacts_directory  -- Where the UDF jars are
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     month                -- YYYY-MM to compute statistics for
--
-- Usage:
--     hive -f insert_geowiki_monthly_data.hql                                              \
--         -d source_table=wmf.geowiki_daily                                                \
--         -d destination_table=wmf.geowiki_monthly                                         \
--         -d month=2018-02
--

INSERT OVERWRITE TABLE ${destination_table}
       PARTITION (month='${month}')

     SELECT wiki_db,
            country_code,
            user_is_anonymous as users_are_anonymous,
            activity_level,
            count(*) as distinct_editors

       FROM (select wiki_db,
                    country_code,
                    user_is_anonymous,
                    user_id_or_ip,
                    case
                        when sum(edit_count) >= 100 then '100 or more'
                        when sum(edit_count) >= 5 then '5 to 99'
                        else '1 to 4'
                    end as activity_level
               from ${source_table}
              where month = '${month}'
              group by wiki_db,
                    country_code,
                    user_is_anonymous,
                    user_id_or_ip
            ) editors_with_monthly_activity

      GROUP BY wiki_db,
            country_code,
            activity_level,
            user_is_anonymous
;
