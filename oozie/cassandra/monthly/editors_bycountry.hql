-- Parameters:
--     source_table          -- Fully qualified table name to compute from.
--     destination_directory -- HDFS path to write output files.
--     year                  -- Year of partition to compute from.
--     month                 -- Month of partition to compute from.
--     separator             -- Separator for values.
--
-- Usage:
--     hive -f editors_bycountry.hql                              \
--         -d source_table=wmf.geoeditors_public_monthly          \
--         -d destination_directory=/tmp/editors_bycountry        \
--         -d year=2018                                           \
--         -d month=1                                             \
--         -d separator='\t'

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

INSERT OVERWRITE DIRECTORY "${destination_directory}"
-- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
-- works for exports to local directories (see HIVE-5672), we have to
-- prepare the lines by hand through concatenation :-(
SELECT
    CONCAT_WS("${separator}",
        project,
        CASE
            WHEN activity_level = "5 to 99" THEN "5..99-edits"
            WHEN activity_level = "100 or more" THEN "100..-edits"
            ELSE NULL
        END,
        "${year}",
        LPAD("${month}", 2, "0"),
        CONCAT('[',
            CONCAT_WS(
                ',',
                COLLECT_SET(
                    CONCAT(
                        '{"country":"', country_code,
                        '","editors-ceil":', CAST(editors_ceil AS STRING), '}'
                    )
                )
            ),
        ']')
    )
FROM ${source_table}
WHERE
    month = CONCAT("${year}-", LPAD("${month}", 2, "0"))
GROUP BY
    project,
    activity_level,
    month
;
