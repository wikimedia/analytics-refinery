-- Compute daily session length data from session tick events.
-- See README.md for more info.
--
-- Run this job from spark3-sql or Airflow.
--
-- Parameters:
--     source_table               -- Table to read session tick data from
--     destination_table          -- Table to write session length data to
--     year                       -- Unpadded year of the date to compute
--     month                      -- Unpadded month of the date to compute
--     day                        -- Unpadded day of the date to compute
--     output_files_cardinality   -- Number of files in the output
SET parquet.compression = SNAPPY;

WITH
    base_data AS (
        SELECT
            CASE
                WHEN normalized_host.project = '-' THEN
                    normalized_host.project_family
                ELSE
                    CONCAT(
                        normalized_host.project,
                        '.',
                        normalized_host.project_family
                    )
            END as wiki,
            tick
        FROM ${source_table}
        WHERE
            year = ${year} AND
            month = ${month} AND
            day = ${day} AND
            is_wmf_domain AND
            test IS NULL
    ),
    indexed AS (
        SELECT
            -- On next line, the ordering is irrelevant to us. But mandatory,
            -- for Spark SQL.
            row_number() OVER (PARTITION BY wiki, tick ORDER BY 1) as index,
            wiki,
            tick
        FROM base_data
    ),
    sessionized AS (
        SELECT
            wiki,
            max(tick) AS session_length
        FROM indexed
        GROUP BY
            index,
            wiki
    )

INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year = ${year}, month = ${month}, day = ${day})
    -- The result is <<1MB, so 1 file is enough.
    SELECT /*+ COALESCE(1) */
        wiki,
        session_length,
        -- Multiplying by 10 to compensate for static 10% sampling.
        -- FIXME Once the sampling rate comes as a field in the events,
        -- multiply this count by (1 / sampling_rate) instead.
        COUNT(*) * 10 AS session_count
    FROM sessionized
    GROUP BY
        wiki,
        session_length
    ORDER BY
        wiki,
        session_length
    -- This limit is a guard against bugs upstream.
    LIMIT 1000000
;
