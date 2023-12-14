-- Parameters:
--     source_table         -- Fully qualified table name to compute the
--                             aggregation for.
--     webrequest_source    -- Varnish cluster that handled request.
--     destination_table    -- Fully qualified table name to fill in
--                             aggregated values.
--     coalesce_partitions  -- Number of partitions to write
--     year                 -- Year of partition to compute aggregation
--                             for.
--     month                -- Month of partition to compute aggregation
--                             for.
--     day                  -- Day of partition to compute aggregation
--                             for.
--     hour                 -- Hour of partition to compute aggregation
--                             for.
--
-- spark3-sql -f aqs_hourly_iceberg.hql                         \
--            -d source_table=wmf.webrequest                    \
--            -d webrequest_source=text                         \
--            -d destination_table=wmf_traffic.aqs_hourly       \
--            -d coalesce_partitions=1                          \
--            -d year=2021                                      \
--            -d month=3                                        \
--            -d day=3                                          \
--            -d hour=0


-- Delete existing data for the period to prevent duplication of data in case of recomputation
DELETE FROM ${destination_table}
WHERE hour = CAST(CONCAT(
    LPAD(${year}, 4, '0'), '-',
    LPAD(${month}, 2, '0'), '-',
    LPAD(${day}, 2, '0'), ' ',
    LPAD(${hour}, 2, '0'), ':00:00'
) AS TIMESTAMP);

-- Compute data for the period
INSERT INTO TABLE ${destination_table}
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        cache_status,
        http_status,
        http_method,
        response_size,
        uri_host,
        uri_path,
        COUNT(1) AS request_count,
        CAST(CONCAT(
            LPAD(${year}, 4, '0'), '-',
            LPAD(${month}, 2, '0'), '-',
            LPAD(${day}, 2, '0'), ' ',
            LPAD(${hour}, 2, '0'), ':00:00'
        ) AS TIMESTAMP)
        AS hour
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
    ORDER BY hour
;
