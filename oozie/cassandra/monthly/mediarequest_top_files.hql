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
--         -d year=2015                                            \
--         -d month=5
--


-- Throttle the maximum number of concurrent reducers to 350
-- This makes the reduce step longer but prevents the job to
-- saturate network links.
SET mapreduce.job.running.reduce.limit = 350;

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


WITH ranked AS (
    SELECT
        referer,
        file_path,
        media_classification,
        year,
        month,
        requests,
        rank() OVER (PARTITION BY referer, media_classification, year, month ORDER BY requests DESC) as rank,
        row_number() OVER (PARTITION BY referer, media_classification, year, month ORDER BY requests DESC) as rn
    FROM (
        SELECT
            COALESCE(IF(referer = 'external (search engine)', 'search-engine', referer), 'all-referers') referer,
            regexp_replace(base_name, '${separator}', '') file_path,
            COALESCE(media_classification, 'all-media-types') media_classification,
            LPAD(year, 4, "0") as year,
            LPAD(month, 2, "0") as month,
            SUM(request_count) as requests
        FROM ${source_table}
        WHERE
            year = ${year}
            AND month = ${month}
            AND agent_type = 'user'
        GROUP BY referer, regexp_replace(base_name, '${separator}', ''), media_classification, year, month
        GROUPING SETS (
            (
                year,
                month,
                referer,
                regexp_replace(base_name, '${separator}', ''),
                media_classification
            ), (
                year,
                month,
                regexp_replace(base_name, '${separator}', ''),
                media_classification
            ), (
                year,
                month,
                referer,
                regexp_replace(base_name, '${separator}', '')
            ), (
                year,
                month,
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
        rank as max_rank
    FROM ranked
    WHERE
        rn = 1001
    GROUP BY
        referer,
        media_classification,
        year,
        month,
        rank
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '${separator}'
SELECT
    referer,
    media_classification,
    year,
    month,
    'all-days',
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
)
WHERE ranked.rank < COALESCE(max_rank.max_rank, 1001)
GROUP BY
    ranked.referer,
    ranked.media_classification,
    ranked.year,
    ranked.month
;
