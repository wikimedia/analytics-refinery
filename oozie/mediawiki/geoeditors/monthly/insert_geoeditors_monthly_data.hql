-- Aggregate editors_daily data into the traditional kind of data kept by geoeditors
-- Note: This dataset does NOT contain bots actions and only considers edit actions
--
-- Parameters:
--     refinery_jar_version -- Version of the jar to import for UDFs
--     artifacts_directory  -- Where the UDF jars are
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     month                -- YYYY-MM to compute statistics for
--
-- Usage:
--     hive -f insert_geoeditors_monthly_data.hql        \
--         -d source_table=wmf.editors_daily             \
--         -d destination_table=wmf.geoeditors_monthly   \
--         -d month=2018-02
--

WITH overall AS (

     SELECT wiki_db,
            country_code,
            user_is_anonymous as users_are_anonymous,
            activity_level,
            count(*) as distinct_editors

       FROM (select wiki_db,
                    country_code,
                    user_is_anonymous,
                    user_fingerprint_or_name,
                    case
                        when sum(edit_count) >= 100 then '100 or more'
                        when sum(edit_count) >= 5 then '5 to 99'
                        else '1 to 4'
                    end as activity_level
               from ${source_table}
              where month = '${month}'
                    -- Filter out bot actions and non-edit actions
                    and size(user_is_bot_by) = 0
                    and action_type IN (0, 1)
              group by wiki_db,
                    country_code,
                    user_is_anonymous,
                    user_fingerprint_or_name
            ) editors_with_monthly_activity

      GROUP BY wiki_db,
            country_code,
            activity_level,
            user_is_anonymous

), only_ns0 as (

     SELECT wiki_db,
            country_code,
            user_is_anonymous as users_are_anonymous,
            activity_level,
            count(*) as distinct_editors

       FROM (select wiki_db,
                    country_code,
                    user_is_anonymous,
                    user_fingerprint_or_name,
                    case
                        when sum(namespace_zero_edit_count) >= 100 then '100 or more'
                        when sum(namespace_zero_edit_count) >= 5 then '5 to 99'
                        else '1 to 4'
                    end as activity_level
               from ${source_table}
              where month = '${month}'
                    -- Filter out bot actions, non-edit actions
                    and size(user_is_bot_by) = 0
                    and action_type IN (0, 1)
                    -- Filter out rows having 0 namespace-zero actions
                    and namespace_zero_edit_count > 0
              group by wiki_db,
                    country_code,
                    user_is_anonymous,
                    user_fingerprint_or_name
            ) editors_with_monthly_activity

      GROUP BY wiki_db,
            country_code,
            activity_level,
            user_is_anonymous

)

INSERT OVERWRITE TABLE ${destination_table}
       PARTITION (month='${month}')

     SELECT coalesce(overall.wiki_db, only_ns0.wiki_db),
            coalesce(overall.country_code, only_ns0.country_code),
            coalesce(overall.users_are_anonymous, only_ns0.users_are_anonymous),
            coalesce(overall.activity_level, only_ns0.activity_level),
            coalesce(overall.distinct_editors, 0)  as distinct_editors,
            coalesce(only_ns0.distinct_editors, 0) as namespace_zero_distinct_editors

       FROM overall
                FULL OUTER JOIN
            only_ns0                on overall.wiki_db = only_ns0.wiki_db
                                   and overall.country_code = only_ns0.country_code
                                   and overall.users_are_anonymous = only_ns0.users_are_anonymous
                                   and overall.activity_level = only_ns0.activity_level

;
