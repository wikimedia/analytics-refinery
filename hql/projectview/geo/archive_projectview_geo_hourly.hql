-- This hql is an updated version of refinery/oozie/projectview/geo/archive_projectview_geo_hourly.hql script
-- This new version will no longer take the 'artifacts_directory' and 'refinery_jar_version' as parameters.
-- Instead we now use the 'refinery_hive_jar' parameter.
-- 'refinery_hive_jar' is the full path of the refinery hive jar containing the UDFs.
-- e.g. refinery-hive-jar=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/refinery-hive-0.1.25.jar
--
--
-- Parameters:
--     source_table          -- Fully qualified table name with a
--                              country_code column
--     destination_directory -- Directory to write CSV output to
--     refinery_hive_jar     -- path of hive jar to import for UDFs
--     year                  -- year of partition to compute statistics for.
--     month                 -- month of partition to compute statistics for.
--     day                   -- day of partition to compute statistics for.
--     hour                  -- hour of partition to compute statistics for.
--     coalesce_partitions   -- the number of final partitions.
--
-- Usage:
--     spark-sql --master yarn -f archive_projectview_geo_hourly.hql  \
--         -d source_table=wmf.pageview_hourly                         \
--         -d destination_directory=/wmf/tmp/analytics/example         \
--         -d refinery_hive_jar=/path/to/refinery/hive/jar             \
--         -d year=2015                                                \
--         -d month=6                                                  \
--         -d day=1                                                    \
--         -d hour=1                                                   \
--         -d coalesce_partitions=1                                    \
--
--

ADD JAR ${refinery_hive_jar};
CREATE TEMPORARY FUNCTION country_name as 'org.wikimedia.analytics.refinery.hive.CountryNameUDF';

INSERT OVERWRITE DIRECTORY '${destination_directory}'
    USING csv
    OPTIONS ('compression' 'gzip','sep' '\t')
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        continent,
        country_name(country_code) AS country_name,
        country_code,
        project,
        access_method,
        agent_type,
        sum(view_count) AS view_count
    FROM ${source_table}
    WHERE year=${year} AND month=${month} AND day=${day} AND hour=${hour}
    GROUP BY
        continent,
        country_code,
        project,
        access_method,
        agent_type
    ORDER BY
        continent,
        country_code,
        project,
        access_method,
        agent_type
;
