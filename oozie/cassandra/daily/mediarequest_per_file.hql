-- Parameters:
--     destination_directory -- HDFS path to write output files
--     source_table          -- Fully qualified table name to compute from.
--     separator             -- Separator for values
--     year                  -- year of partition to compute from.
--     month                 -- month of partition to compute from.
--     day                   -- day of partition to compute from.
--
-- Usage:
--     hive -f mediarequest_per_file.hql                       \
--         -d destination_directory=/wmf/tmp/analytics/mediarequest_per_file \
--         -d source_table=wmf.mediarequest                    \
--         -d separator="\t"                                   \
--         -d year=2019                                        \
--         -d month=8                                          \
--         -d day=15
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

WITH per_referer AS (
    SELECT
        IF(referer = 'external (search engine)', 'search-engine', referer) referer,
        regexp_replace(base_name, '${separator}', '') file_path,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"), "00") `timestamp`,
        -- Spider
        CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'spider', request_count, 0)) AS STRING) spider,
        -- User
        CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'user', request_count, 0)) AS STRING) `user`
    FROM
        ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY
        referer,
        regexp_replace(base_name, '${separator}', ''),
        year,
        month,
        day
), all_referers AS (
    SELECT
        'all-referers' referer,
        regexp_replace(base_name, '${separator}', '') file_path,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"), "00") `timestamp`,
        -- Spider
        CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'spider', request_count, 0)) AS STRING) spider,
        -- User
        CAST(SUM( IF (COALESCE(agent_type, 'all-agents') = 'user', request_count, 0)) AS STRING) `user`
    FROM
        ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY
        regexp_replace(base_name, '${separator}', ''),
        year,
        month,
        day
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '${separator}'
SELECT * FROM per_referer
UNION ALL
SELECT * FROM all_referers;
