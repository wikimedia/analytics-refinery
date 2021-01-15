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
--         -d destination_directory=/wmf/tmp/analytics/mediarequest_per_project \
--         -d source_table=wmf.mediarequest                       \
--         -d separator=\t                                        \
--         -d year=2015                                           \
--         -d month=5                                             \
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '${separator}'
SELECT
    COALESCE(IF(referer = 'external (search engine)', 'search-engine', referer), 'all-referers'),
    COALESCE(media_classification, 'all-media-types'),
    COALESCE(agent_type, 'all-agents'),
    CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), "0100"),
    CAST(SUM(request_count) AS STRING)
FROM
    ${source_table}
WHERE
    year = ${year}
    AND month = ${month}
GROUP BY
    referer,
    media_classification,
    agent_type,
    year,
    month
GROUPING SETS (
    (
        referer,
        media_classification,
        agent_type,
        year,
        month
    ),(
        referer,
        media_classification,
        year,
        month
    ),(
        referer,
        agent_type,
        year,
        month
    ),(
        media_classification,
        agent_type,
        year,
        month
    ),(
        media_classification,
        year,
        month
    ),(
        agent_type,
        year,
        month
    ),(
        referer,
        year,
        month
    ),(
        year,
        month
    )
);