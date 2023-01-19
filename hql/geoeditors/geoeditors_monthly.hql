-- Aggregate editors_daily data into the traditional kind of data kept by geoeditors
-- Note: This dataset does NOT contain bots actions and only considers edit actions
--
-- Parameters:
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     month                -- YYYY-MM to compute statistics for
--     coalesce_partitions  -- Number of partitions to write
--
-- Usage:
--     hive -f geoeditors_monthly.hql         \
--          -d source_table=wmf.editors_daily             \
--          -d destination_table=wmf.geoeditors_monthly   \
--          -d month=2022-02
--          -d coalesce_partitions=1
--
-- NOTE: as discussed in https://phabricator.wikimedia.org/T324907#8541865,
--   entries with cuc_type = 3 are being moved into separate tables
--   In this job, action_type IN (0, 1) below means we don't use type 3,
--   but if this becomes useful in the future, it'll need an update here.

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

     SELECT /*+ COALESCE(${coalesce_partitions}) */
            coalesce(overall.wiki_db, only_ns0.wiki_db),
            coalesce(overall.country_code, only_ns0.country_code),
            coalesce(overall.users_are_anonymous, only_ns0.users_are_anonymous),
            coalesce(overall.activity_level, only_ns0.activity_level),
            coalesce(overall.distinct_editors, 0)  AS distinct_editors,
            coalesce(only_ns0.distinct_editors, 0) AS namespace_zero_distinct_editors
       FROM overall
       FULL OUTER JOIN only_ns0 ON overall.wiki_db = only_ns0.wiki_db
                                AND overall.country_code = only_ns0.country_code
                                AND overall.users_are_anonymous = only_ns0.users_are_anonymous
                                AND overall.activity_level = only_ns0.activity_level
;
