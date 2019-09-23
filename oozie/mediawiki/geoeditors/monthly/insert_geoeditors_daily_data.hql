-- Geolocate and group edits by wiki, country, and user
-- This data will be used to generate monthly geoeditors reports,
-- and deleted after 90 days for privacy reasons.
--
-- Parameters:
--     refinery_jar_version -- Version of the jar to import for UDFs
--     artifacts_directory  -- Where the UDF jars are
--     source_table         -- Read raw data from here
--     user_history_table   -- Look up whether a user is a bot here
--     destination_table    -- Insert results here
--     month                -- YYYY-MM to compute statistics for
--
-- Usage:
--     hive -f insert_geoeditors_daily_data.hql                                             \
--         -d refinery_jar_version=0.0.58                                                   \
--         -d artifacts_directory=hdfs://analytics-hadoop/wmf/refinery/current/artifacts    \
--         -d source_table=wmf_raw.mediawiki_private_cu_changes                             \
--         -d user_history_table=wmf.mediawiki_user_history                                 \
--         -d destination_table=wmf.geoeditors_daily                                        \
--         -d month=2018-02
--
-- TODO: Change user_groups to user_groups_historical, adding user_groups_latest to help searchers find this line
ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
CREATE TEMPORARY FUNCTION geocode as 'org.wikimedia.analytics.refinery.hive.GeocodedDataUDF';
CREATE TEMPORARY FUNCTION network_origin as 'org.wikimedia.analytics.refinery.hive.GetNetworkOriginUDF';


INSERT OVERWRITE TABLE ${destination_table}
       PARTITION (month='${month}')

     SELECT wiki_db,
            country_code,
            user_fingerprint_or_id,
            user_is_anonymous,
            date,
            count(*) as edit_count,
            sum(page_is_namespace_zero) as namespace_zero_edit_count,
            network_origin

       FROM (select cuc.wiki_db,
                    geocode(cuc_ip)['country_code'] as country_code,
                    network_origin(cuc_ip) as network_origin,
                    if(cuc_user = 0, md5(concat(cuc_ip, cuc_agent)), cuc_user) as user_fingerprint_or_id,
                    if(cuc_user = 0, 1, 0) as user_is_anonymous,
                    if(cuc_namespace = 0, 1, 0) as page_is_namespace_zero,
                    concat(
                        substring(cuc_timestamp, 0, 4), '-',
                        substring(cuc_timestamp, 5, 2), '-',
                        substring(cuc_timestamp, 7, 2)
                    ) as date

               from ${source_table} cuc
                        left join
                    ${user_history_table} uh    on uh.snapshot = '${month}'
                                                and uh.wiki_db = cuc.wiki_db
                                                and user_id = cuc_user

              where cuc.month='${month}'
                and cuc_type in (0, 1)
                and (   uh.user_id is null
                    or  (   cuc_timestamp between
                                coalesce(start_timestamp, '20010101000000') and
                                coalesce(end_timestamp, '99999999999999')
                        -- Removing bots
                        and (SIZE(is_bot_by) = 0)
                        )
                    )
            ) geolocated_edits

      GROUP BY wiki_db,
            country_code,
            date,
            user_fingerprint_or_id,
            user_is_anonymous,
            network_origin
;
