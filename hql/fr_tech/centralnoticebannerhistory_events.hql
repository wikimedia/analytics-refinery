-- Extracts one hour of events from centralnoticebannerhistory to be loaded to fr tech's minio

-- Usage:
--     spark3-sql -f centralnoticebannerhistory.hql \
--         -d source_table=event_sanitized.centralnoticebannerhistory \
--         -d destination_table=wmf_fr_tech.centralnoticebannerhistory \
--         -d destination_directory=/wmf/data/wmf_fr_tech/centralnoticebannerhistory \
--         -d coalesce_partitions=1 \
--         -d year=2023 \
--         -d month=1 \
--         -d day=1 \
--         -d hour=1 

CREATE TABLE IF NOT EXISTS ${destination_table} (
    `dt`                             string,
    `event_id`                       string,
    `sample_rate`                    double,
    `log_length`                     int,
    `banner_history_log`             array<string>
)
USING PARQUET
PARTITIONED BY (
    `year`     int,
    `month`    int,
    `day`      int,
    `hour`     int
)
OPTIONS ('compression'='gzip')
LOCATION '${destination_directory}';

SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;


INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year},month=${month},day=${day}, hour=${hour})
    SELECT /*+ COALESCE(${coalesce_partitions}) */
        dt,
        event.i as event_id,
        event.r as sample_rate,
        event.n as log_length,
        event.l as banner_history_log
    FROM
        ${source_table}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
        AND hour=${hour}
