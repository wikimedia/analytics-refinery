-- Compute daily session length data from session tick events and write it to the Iceberg table.
-- See README.md for more info.
--
-- Run this job from spark3-sql or Airflow.
--
-- Parameters:
--   source_table        -- Table to read session tick data from
--   destination_table   -- Iceberg table to write session length data to
--   day                 -- day of the date to compute (YYYY-MM-DD)
--   coalesce_partitions -- Number of partitions to write at query end
--
-- Usage:
--   spark3-sql \
--     -f session_length_iceberg.hql \
--     -d source_table=event.mediawiki_client_session_tick \
--     -d destination_table=wmf_traffic.session_length \
--     -d coalesce_partitions=1 \
--     -d day=2023-01-30

DELETE FROM ${destination_table}
    WHERE day = TO_DATE('${day}', 'yyyy-MM-dd');

WITH
    base_data AS (
        SELECT
            CASE
                WHEN normalized_host.project = '-'
                    THEN normalized_host.project_family
                ELSE
                    CONCAT(
                            normalized_host.project,
                            '.',
                            normalized_host.project_family
                    )
                END as wiki,
            tick
        FROM ${source_table}
        WHERE year = YEAR(TO_DATE('${day}', 'yyyy-MM-dd'))
            AND month = MONTH(TO_DATE('${day}', 'yyyy-MM-dd'))
            AND day = DAY(TO_DATE('${day}', 'yyyy-MM-dd'))
            AND is_wmf_domain
            AND test IS NULL
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

INSERT INTO ${destination_table}

SELECT /*+ COALESCE(${coalesce_partitions}) */
    wiki,
    session_length,
    -- Multiplying by 10 to compensate for static 10% sampling.
    -- FIXME Once the sampling rate comes as a field in the events,
    -- multiply this count by (1 / sampling_rate) instead.
    COUNT(*) * 10 AS session_count,
    TO_DATE('${day}', 'yyyy-MM-dd') AS day
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
