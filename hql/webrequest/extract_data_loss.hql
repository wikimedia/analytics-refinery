-- Produce a one-liner report in a directory if incomplete data or data loss in webrequest statistics hourly for a given
-- source and hour is above the given thresholds.
--
-- The file with the line will look like: part-00000-d60c80c5-ebe0-4b73-8c47-1fd896ab403b-c000.csv
-- If both incomplete data and data loss are below thresholds, the file will exist, but will be empty.
--
-- Parameters:
--     source_table               -- Fully qualified table name containing the hourly statistics to read.
--     target                     -- HDFS Path where to write the file. If this path exists, it will get overwritten.
--                                   In production, it should include the full HDFS url `hdfs://name_node/...`.
--     incomplete_data_threshold  -- The threshold for incomplete data not to be above in %.
--     data_loss_threshold        -- The threshold for data loss not to be above in %.
--     webrequest_source          -- webrequest_source for the partition to check data loss for.
--     year                       -- year for the partition to check data loss for.
--     month                      -- month for the partition to check data loss for.
--     day                        -- day for the partition to check data loss for.
--     hour                       -- hour for the partition to check data loss for.
--
-- Usage example:
--     spark3-sql \
--         --master yarn \
--         --executor-memory 1G \
--         --executor-cores 1 \
--         --driver-memory 2G \
--         --driver-cores 1 \
--         --conf spark.dynamicAllocation.maxExecutors=1 \
--         --name test-extract-data-loss \
--         -f extract_data_loss.hql \
--         -d source_table=wmf_raw.webrequest_sequence_stats_hourly \
--         -d target=hdfs:///user/user1/extract_data_loss \
--         -d incomplete_data_threshold=2 \
--         -d data_loss_threshold=2 \
--         -d webrequest_source=text \
--         -d year=2023 \
--         -d month=3 \
--         -d day=20 \
--         -d hour=0
--

WITH
    data_loss AS (
        SELECT
            count_lost AS count,
            percent_lost AS `percent`
        FROM
            ${source_table}
        WHERE
            webrequest_source='${webrequest_source}' AND
            year=${year} AND month=${month} AND day=${day} AND hour=${hour}
    ),
    incomplete_data AS (
        SELECT
            count_incomplete AS count,
            ((count_incomplete / (count_actual + count_incomplete)) * 100.0) AS `percent`
        FROM
            ${source_table}
        WHERE
            webrequest_source='${webrequest_source}' AND
            year=${year} AND month=${month} AND day=${day} AND hour=${hour}
    )

INSERT OVERWRITE DIRECTORY '${target}'
USING CSV
OPTIONS (
    'compression' 'none',
    'header' 'False',
    'delimiter' '\\t'
)
SELECT /*+ COALESCE(1) */
    incomplete_data.count || ' incomplete requests' as `requests_with_incomplete_records`,
    ROUND(incomplete_data.`percent`, 3) || '% incomplete requests to total' as `requests_with_incomplete_records_proportion`,
    data_loss.count || ' lost requests' as lost_requests,
    ROUND(data_loss.`percent`, 3) || '% lost request to valid requests' as lost_request_proportion
FROM
    data_loss, incomplete_data
WHERE
    data_loss.`percent` > ${data_loss_threshold} OR
    incomplete_data.`percent` > ${incomplete_data_threshold}
;
