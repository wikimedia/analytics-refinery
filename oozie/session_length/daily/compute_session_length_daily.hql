-- FIXME Deprecated by hql/session_length/daily + Airflow
--
-- Compute daily session length data from session tick events.
-- See README.md for more info.
--
-- Parameters:
--     source_table        -- Table to read session tick data from
--     destination_table   -- Table to write session length data to
--     year                -- unpadded year of the date to compute
--     month               -- unpadded month of the date to compute
--     day                 -- unpadded day of the date to compute
--
-- Usage:
--     hive -f compute_session_length_daily.hql \
--         -d refinery_hive_jar=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/org/wikimedia/analytics/refinery/refinery-hive-0.1.2.jar \
--         -d source_table=event.mediawiki_client_session_tick \
--         -d destination_table=mforns.session_length_daily \
--         -d year=2021 \
--         -d month=1 \
--         -d day=1

ADD JAR ${refinery_hive_jar};
CREATE TEMPORARY FUNCTION host_properties AS 'org.wikimedia.analytics.refinery.hive.GetHostPropertiesUDF';

WITH
    base_data AS (
        SELECT
            CASE
                WHEN (host_properties(meta.domain).project = '-') THEN
                    host_properties(meta.domain).project_family
                ELSE
                    CONCAT(
                        host_properties(meta.domain).project,
                        '.',
                        host_properties(meta.domain).project_family
                    )
            END as wiki,
            tick
        FROM ${source_table}
        WHERE
            year = ${year} AND
            month = ${month} AND
            day = ${day} AND
            is_wmf_domain
    ),
    indexed AS (
        SELECT
            row_number() OVER (PARTITION BY wiki, tick) as index,
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
    SELECT
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
    -- Use order by to enforce single reducer and thus single output file.
    ORDER BY
        wiki,
        session_length
    LIMIT 1000000
;
