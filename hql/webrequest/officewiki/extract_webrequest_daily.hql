-- Copy webrequests with hostname 'office.wikimedia.org' into a daily table
--
-- Parameters:
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     year                 -- execution year
--     month                -- execution month
--     day                  -- execution day
--
-- Usage:
--     hive -f extract_webrequest_daily.hql                         \
--          -d refinery_hive_jar='hdfs://.../refinery-hive-...jar'  \
--          -d source_table=wmf.webrequest                          \
--          -d destination_table=wmf.officewiki_webrequest_daily    \
--          -d year=2022                                            \
--          -d month=3                                              \
--          -d day=1
--
ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION get_actor_signature AS 'org.wikimedia.analytics.refinery.hive.GetActorSignatureUDF';

 insert OVERWRITE TABLE ${destination_table}
        PARTITION (year='${year}', month='${month}', day='${day}')

 select uri_path,
        get_actor_signature(ip, user_agent, accept_language, uri_host, uri_query, x_analytics_map) as actor_signature,
        http_status

   from ${source_table}

  where year=${year} and month=${month} and day=${day}
    and webrequest_source = 'text'
    and uri_host = 'office.wikimedia.org'
;
