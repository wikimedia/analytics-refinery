-- Geolocate and group edits by wiki, country, user, user_is_bot_by and action_type.
-- This data will be used to generate monthly geoeditors reports,
-- and deleted after 90 days for privacy reasons.
--
-- Parameters:
--     refinery_hive_jar    -- Path to refinery hive JAR
--     source_table         -- Read raw data from here
--     user_history_table   -- Look up whether a user is a bot here
--     destination_table    -- Insert results here
--     month                -- YYYY-MM to compute statistics for
--     coalesce_partitions  -- Number of partitions to write
--
-- Usage:
--     spark3-sql -f editors_daily_monthly.hql                                                       \
--                -d refinery_hive_jar=hdfs://analytics-hadoop/some/path/to/refinery-hive-0.1.9.jar  \
--                -d source_table=wmf_raw.mediawiki_private_cu_changes                               \
--                -d user_history_table=wmf.mediawiki_user_history                                   \
--                -d destination_table=wmf.editors_daily                                             \
--                -d month=2022-02                                                                   \
--                -d coalesce_partitions=1
--
-- NOTE: as discussed in https://phabricator.wikimedia.org/T324907#8541865,
--   entries with cuc_type = 3 are being moved into separate tables
--   currently these are not used downstream, but if they become useful, hopefully
--   someone finds this comment

ADD JAR ${refinery_hive_jar};
CREATE TEMPORARY FUNCTION geocode as 'org.wikimedia.analytics.refinery.hive.GeocodedDataUDF';
CREATE TEMPORARY FUNCTION network_origin as 'org.wikimedia.analytics.refinery.hive.GetNetworkOriginUDF';

INSERT OVERWRITE TABLE ${destination_table}
       PARTITION (month='${month}')

     SELECT /*+ COALESCE(${coalesce_partitions}) */
            wiki_db,
            country_code,
            user_fingerprint_or_name,
            user_is_anonymous,
            `date`,
            count(*) as edit_count,
            sum(page_is_namespace_zero) as namespace_zero_edit_count,
            network_origin,
            user_is_bot_by,
            action_type

       FROM (select cuc.wiki_db,
                    geocode(cuc_ip)['country_code'] as country_code,
                    network_origin(cuc_ip) as network_origin,
                    coalesce(is_bot_by_historical, array()) as user_is_bot_by,
                    cuc_type as action_type,
                    if(cuc_user = 0, md5(concat(cuc_ip, cuc_agent)), cuc_user_text) as user_fingerprint_or_name,
                    if(cuc_user = 0, TRUE, FALSE) as user_is_anonymous,
                    -- Using integers for the sum above
                    if(cuc_namespace = 0, 1, 0) as page_is_namespace_zero,
                    concat(
                        substring(cuc_timestamp, 0, 4), '-',
                        substring(cuc_timestamp, 5, 2), '-',
                        substring(cuc_timestamp, 7, 2)
                    ) as `date`
               from ${source_table} cuc
               left join ${user_history_table} uh on uh.snapshot = cuc.month
                and uh.wiki_db = cuc.wiki_db
                and user_id = cuc_user
              where cuc.month='${month}'
                and (
                    uh.user_id is null  --  either no user-event match

                    -- Keep user-event whose start and end date encompass
                    -- cuc_timestamp for historically correct values
                    or cuc_timestamp between
                          coalesce(start_timestamp, '20010101000000') and
                          coalesce(end_timestamp, '99999999999999')
                    )
            ) geolocated_edits

      GROUP BY wiki_db,
            country_code,
            `date`,
            user_fingerprint_or_name,
            user_is_anonymous,
            network_origin,
            user_is_bot_by,
            action_type
;
