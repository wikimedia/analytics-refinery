-- Aggregate editors_daily data into monthly summaries of unique editor counts by country, regardless of wiki project.
-- Note: This dataset does NOT contain bots actions and only considers edit actions
--
-- Parameters:
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     month                -- YYYY-MM to compute statistics for
--     coalesce_partitions  -- Number of partitions to write
--
-- Usage:
--     spark3-sql -f unique_editors_by_country_monthly.hql         \
--                -d source_table=wmf.editors_daily             \
--                -d destination_table=wmf.unique_editors_by_country_monthly   \
--                -d month=2022-02    \
--                -d coalesce_partitions=1
--
-- NOTE: as discussed in https://phabricator.wikimedia.org/T324907#8541865,
--   entries with cuc_type = 3 are being moved into separate tables
--   currently these are not used downstream, but if they become useful, hopefully
--   someone finds this comment

WITH filtered_source AS (
    SELECT
        country_code,
        user_is_anonymous,
        user_is_permanent,
        user_is_temporary,
        user_fingerprint_or_name,
        edit_count,
        namespace_zero_edit_count
    FROM ${source_table}
    WHERE
        month = '${month}'
        -- Filter out bot actions and non-edit actions
        AND SIZE(user_is_bot_by) = 0
        AND action_type IN (0, 1)
), overall AS (
    SELECT
        country_code,
        user_is_anonymous AS users_are_anonymous,
        user_is_permanent AS users_are_permanent,
        user_is_temporary AS users_are_temporary,
        activity_level,
        COUNT(*) AS distinct_editors
    FROM (
        SELECT
            country_code,
            user_is_anonymous,
            user_is_permanent,
            user_is_temporary,
            user_fingerprint_or_name,
            CASE
                WHEN sum(edit_count) >= 100 then '100 or more'
                WHEN sum(edit_count) >= 5 then '5 to 99'
                ELSE '1 to 4'
            END AS activity_level
            FROM filtered_source
            GROUP BY
                country_code,
                user_is_anonymous,
                user_is_permanent,
                user_is_temporary,
                user_fingerprint_or_name
    ) editors_with_monthly_activity
    GROUP BY
        country_code,
        activity_level,
        user_is_anonymous,
        user_is_permanent,
        user_is_temporary
), only_ns0 as (
    SELECT
        country_code,
        user_is_anonymous as users_are_anonymous,
        user_is_permanent as users_are_permanent,
        user_is_temporary as users_are_temporary,
        activity_level,
        COUNT(*) AS distinct_editors
    FROM (
        SELECT
            country_code,
            user_is_anonymous,
            user_is_permanent,
            user_is_temporary,
            user_fingerprint_or_name,
            CASE
                WHEN SUM(namespace_zero_edit_count) >= 100 then '100 or more'
                WHEN SUM(namespace_zero_edit_count) >= 5 then '5 to 99'
                ELSE '1 to 4'
            END AS activity_level
        FROM filtered_source
        WHERE namespace_zero_edit_count > 0
        GROUP BY
            country_code,
            user_is_anonymous,
            user_is_permanent,
            user_is_temporary,
            user_fingerprint_or_name
    ) editors_with_monthly_activity
    GROUP BY
        country_code,
        activity_level,
        user_is_anonymous,
        user_is_permanent,
        user_is_temporary
)

INSERT OVERWRITE TABLE ${destination_table}
PARTITION (month='${month}')
SELECT /*+ COALESCE(${coalesce_partitions}) */
    COALESCE(overall.country_code, only_ns0.country_code),
    COALESCE(overall.users_are_anonymous, only_ns0.users_are_anonymous),
    COALESCE(overall.users_are_permanent, only_ns0.users_are_permanent),
    COALESCE(overall.users_are_temporary, only_ns0.users_are_temporary),
    COALESCE(overall.activity_level, only_ns0.activity_level),
    COALESCE(overall.distinct_editors, 0)  AS distinct_editors,
    COALESCE(only_ns0.distinct_editors, 0) AS namespace_zero_distinct_editors
FROM overall
FULL OUTER JOIN only_ns0 ON overall.country_code = only_ns0.country_code
                         AND overall.users_are_anonymous = only_ns0.users_are_anonymous
                         AND overall.users_are_permanent = only_ns0.users_are_permanent
                         AND overall.users_are_temporary = only_ns0.users_are_temporary
                         AND overall.activity_level = only_ns0.activity_level
;
