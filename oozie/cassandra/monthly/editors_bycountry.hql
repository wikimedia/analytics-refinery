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
--         -d destination_directory=/wmf/tmp/analytics/editors_bycountry        \
--         -d year=2018                                           \
--         -d month=1                                             \
--         -d separator='\t'

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

-- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
-- works for exports to local directories (see HIVE-5672), we have to
-- prepare the lines by hand through concatenation :-(
WITH prepared_editors_by_country AS (
    SELECT
        project,
        activity_level,
        month,
        editors_ceil,
        country_code
    FROM ${source_table}
    WHERE
        month = CONCAT("${year}-", LPAD("${month}", 2, "0"))
    -- This is secondary sorting in Hive:
    --  * It uses SORT BY, not ORDER BY, to sort in each reducer instead of globally
    --  * The sorting-key is a superset of the grouping-key
    DISTRIBUTE BY
        project,
        activity_level,
        month
    SORT BY
        project,
        activity_level,
        month,
        editors_ceil DESC,
        country_code
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
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
FROM prepared_editors_by_country
GROUP BY
    project,
    activity_level,
    month
;
