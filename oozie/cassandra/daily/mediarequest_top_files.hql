-- Parameters:
--     destination_directory  -- HDFS path to write output files
--     source_table           -- Fully qualified table name to compute from.
--     separator              -- Separator for values
--     year                   -- year of partition to compute from.
--     month                  -- month of partition to compute from.
--
-- Usage:
--     hive -f mediarequest_top_files.hql                          \
--         -d destination_directory=/wmf/tmp/analytics/mediarequest_top_files    \
--         -d source_table=wmf.mediarequest                        \
--         -d separator=\t                                         \
--         -d year=2019                                            \
--         -d month=7                                              \
--         -d day=1                                                \
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


WITH ranked AS (
    SELECT
        referer,
        file_path,
        media_classification,
        year,
        month,
        day,
        requests,
        rank() OVER (PARTITION BY referer, media_classification, year, month, day ORDER BY requests DESC) as rank,
        row_number() OVER (PARTITION BY referer, media_classification, year, month, day ORDER BY requests DESC) as rn
    FROM (
        SELECT
            COALESCE(IF(referer = 'external (search engine)', 'search-engine', referer), 'all-referers') referer,
            regexp_replace(base_name, '${separator}', '') file_path,
            COALESCE(media_classification, 'all-media-types') media_classification,
            LPAD(year, 4, "0") as year,
            LPAD(month, 2, "0") as month,
            LPAD(day, 2, "0") as day,
            SUM(request_count) as requests
        FROM ${source_table}
        WHERE
            year = ${year}
            AND month = ${month}
            AND day = ${day}
            AND agent_type = 'user'
        GROUP BY referer, regexp_replace(base_name, '${separator}', ''), media_classification, year, month, day
        GROUPING SETS (
            (
                year,
                month,
                day,
                referer,
                regexp_replace(base_name, '${separator}', ''),
                media_classification
            ), (
                year,
                month,
                day,
                regexp_replace(base_name, '${separator}', ''),
                media_classification
            ), (
                year,
                month,
                day,
                referer,
                regexp_replace(base_name, '${separator}', '')
            ), (
                year,
                month,
                day,
                regexp_replace(base_name, '${separator}', '')
            )
        )
    ) raw
),
max_rank AS (
    SELECT
        referer,
        media_classification,
        year,
        month,
        day,
        rank as max_rank
    FROM ranked
    WHERE
        rn = 1001
    GROUP BY
        referer,
        media_classification,
        year,
        month,
        day,
        rank
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '${separator}'
SELECT
    referer,
    media_classification,
    year,
    month,
    day,
    CONCAT('[',
        CONCAT_WS(',', collect_set(
            CONCAT('{"file_path":"', regexp_replace(file_path, '"', '\\\\"'),
                '","requests":', CAST(requests AS STRING),
                ',"rank":', CAST(rank AS STRING), '}'))
        ),']')
FROM ranked
LEFT JOIN max_rank ON (
    ranked.referer = max_rank.referer
    AND ranked.media_classification = max_rank.media_classification
    AND ranked.year = max_rank.year
    AND ranked.month = max_rank.month
    AND ranked.day = max_rank.day
)
WHERE ranked.rank < COALESCE(max_rank.max_rank, 1001)
GROUP BY
    ranked.referer,
    ranked.media_classification,
    ranked.year,
    ranked.month,
    ranked.day
;
