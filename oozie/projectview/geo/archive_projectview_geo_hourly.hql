-- Parameters:
--     source_table          -- Fully qualified table name with a
--                              country_code column
--     destination_directory -- Directory to write CSV output to
--     artifacts_directory   -- The artifact directory where to find
--                              jar files to import for UDFs
--     refinery_jar_version  -- Version of the jar to import for UDFs
--     year                  -- year of partition to compute statistics for.
--     month                 -- month of partition to compute statistics for.
--     day                   -- day of partition to compute statistics for.
--     hour                  -- hour of partition to compute statistics for.
--
-- Usage:
--     hive -f archive_projectview_geo_hourly.hql               \
--         -d source_table=wmf.pageview_hourly                  \
--         -d destination_directory=/wmf/tmp/analytics/example                \
--         -d artifacts_directory=/path/to/refinery/artifacts   \
--         -d refinery_jar_version=X.X.X                        \
--         -d year=2015                                         \
--         -d month=6                                           \
--         -d day=1                                             \
--         -d hour=1
--

ADD JAR ${artifacts_directory}/org/wikimedia/analytics/refinery/refinery-hive-${refinery_jar_version}.jar;
CREATE TEMPORARY FUNCTION country_name as 'org.wikimedia.analytics.refinery.hive.CountryNameUDF';

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

 INSERT OVERWRITE DIRECTORY '${destination_directory}'
 SELECT CONCAT_WS('\t',
            continent,
            country_name,
            country_code,
            project,
            access_method,
            agent_type,
            CAST(view_count AS STRING)
        ) line
   FROM (SELECT continent,
                country_code,
                country_name(country_code) AS country_name,
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

          LIMIT 100000000
        ) `rows`
;
