-- Parameters:
--     destination_directory -- HDFS path to write output files
--     source_table          -- Fully qualified table name to compute from.
--     separator             -- Separator for values
--     year                  -- year of partition to compute from.
--     month                 -- month of partition to compute from.
--     day                   -- day of partition to compute from.
--
-- Usage:
--     hive -f mediarequest_per_referer.hql                       \
--         -d destination_directory=/wmf/tmp/analytics/mediarequest_per_referer \
--         -d source_table=wmf.mediarequest                       \
--         -d separator=\t                                        \
--         -d year=2015                                           \
--         -d month=5                                             \
--         -d day=1                                               \
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '${separator}'
SELECT
    COALESCE(IF(referer = 'external (search engine)', 'search-engine', referer), 'all-referers'),
    COALESCE(media_classification, 'all-media-types'),
    COALESCE(agent_type, 'all-agents'),
    CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"), "00"),
    CAST(SUM(request_count) AS STRING)
FROM
    ${source_table}
WHERE
    year = ${year}
    AND month = ${month}
    AND day = ${day}
GROUP BY
    referer,
    media_classification,
    agent_type,
    year,
    month,
    day
GROUPING SETS (
    (
        referer,
        media_classification,
        agent_type,
        year,
        month,
        day
    ),(
        referer,
        media_classification,
        year,
        month,
        day
    ),(
        media_classification,
        agent_type,
        year,
        month,
        day
    ),(
        referer,
        agent_type,
        year,
        month,
        day
    ),(
        media_classification,
        year,
        month,
        day
    ),(
        referer,
        year,
        month,
        day
    ),(
        agent_type,
        year,
        month,
        day
    ),(
        year,
        month,
        day
    )
);
