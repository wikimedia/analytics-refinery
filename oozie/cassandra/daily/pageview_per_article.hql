-- Parameters:
--     destination_directory -- HDFS path to write output files
--     source_table          -- Fully qualified table name to compute from.
--     separator             -- Separator for values
--     year                  -- year of partition to compute from.
--     month                 -- month of partition to compute from.
--     day                   -- day of partition to compute from.
--
-- Usage:
--     hive -f pageview_per_article.hql                           \
--         -d destination_directory=/wmf/tmp/analytics/pageview_per_article     \
--         -d source_table=wmf.pageview_hourly                    \
--         -d separator=\t                                        \
--         -d year=2015                                           \
--         -d month=5                                             \
--         -d day=1                                               \
--


SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT
        CONCAT_WS('${separator}',
            project,
            regexp_replace(page_title, '${separator}', ''),
            CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"), "00"),
            -- All access - all agents
            CAST(SUM(view_count) AS STRING),
            -- All access - automated
            CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'automated', view_count, 0)) AS STRING),
            -- All access - spider
            CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'spider', view_count, 0)) AS STRING),
            -- All access - user
            CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'user', view_count, 0)) AS STRING),
            -- desktop - all agents
            CAST(SUM( IF (COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'desktop', view_count, 0)) AS STRING),
            -- desktop - automated
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'desktop')
                AND (COALESCE(agent_type, 'all-agents') = 'automated'), view_count, 0)) AS STRING),
            -- desktop - spider
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'desktop')
                AND (COALESCE(agent_type, 'all-agents') = 'spider'), view_count, 0)) AS STRING),
            -- desktop -user
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'desktop')
                AND (COALESCE(agent_type, 'all-agents') = 'user'), view_count, 0)) AS STRING),
            -- mobile app - all agents
            CAST(SUM( IF (COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-app', view_count, 0)) AS STRING),
            -- mobile app - automated
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-app')
                AND (COALESCE(agent_type, 'all-agents') = 'automated'), view_count, 0)) AS STRING),
            -- mobile app - spider
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-app')
                AND (COALESCE(agent_type, 'all-agents') = 'spider'), view_count, 0)) AS STRING),
            -- mobile app - user
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-app')
                AND (COALESCE(agent_type, 'all-agents') = 'user'), view_count, 0)) AS STRING),
            -- mobile web - all agents
            CAST(SUM( IF (COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-web', view_count, 0)) AS STRING),
            -- mobile web - automated
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-web')
                AND (COALESCE(agent_type, 'all-agents') = 'automated'), view_count, 0)) AS STRING),
            -- mobile web - spider
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-web')
                AND (COALESCE(agent_type, 'all-agents') = 'spider'), view_count, 0)) AS STRING),
            -- mobile web - user
            CAST(SUM( IF ((COALESCE(regexp_replace(access_method, ' ', '-'), 'all-access') = 'mobile-web')
                AND (COALESCE(agent_type, 'all-agents') = 'user'), view_count, 0)) AS STRING)
        )

    FROM
        ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY
        project,
        regexp_replace(page_title, '${separator}', ''),
        year,
        month,
        day;
