-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          aggregation for.
--     destination_table -- Fully qualified table name to fill in
--                          aggregated values.
--     year              -- year of partition to compute aggregation
--                          for.
--     month             -- month of partition to compute aggregation
--                          for.
--     day               -- day of partition to compute aggregation
--                          for.
--     hour              -- hour of partition to compute aggregation
--                          for.
--
-- Usage:
--     hive -f aqs_hourly.hql                                     \

--         -d source_table=wmf.webrequest                         \
--         -d destination_table=wmf.aqs_hourly                    \

--         -d year=2015                                           \
--         -d month=11                                            \
--         -d day=1                                               \
--         -d hour=0
--
-- Note: The COALESCE hint was added in Spark 2.4. Here, it is used to set the
--       maximum number of output files.

SET spark.sql.parquet.compression.codec = snappy;

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day},hour=${hour})
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        cache_status,
        http_status,
        http_method,
        response_size,
        uri_host,
        uri_path,
        COUNT(1) AS request_count
    FROM
        ${source_table}
    WHERE webrequest_source = '${webrequest_source}'
        AND year=${year} AND month=${month} AND day=${day} AND hour=${hour}
        AND uri_path LIKE '/api/rest_v1/metrics/%'
    GROUP BY
        cache_status,
        http_status,
        http_method,
        response_size,
        uri_host,
        uri_path
;
