-- TO LAUNCH DAILY TOP MEDIAREQUESTS BACKFILL
--
-- sudo -u analytics oozie job --oozie $OOZIE_URL \
-- -Duser=$USER \
-- -Dcassandra_keyspace=local_group_default_T_mediarequest_per_file \
-- -Dhive_script=../historical/mediacounts/daily/backfill_mediarequest_per_file.hql \
-- -Drefinery_hive_jar_path=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/org/wikimedia/analytics/refinery/refinery-hive-X.X.X.jar \
-- -Dsource_table=wmf.mediacounts \
-- -Ddatasets_file=hdfs://analytics-hadoop/wmf/refinery/current/oozie/mediacounts/datasets.xml \
-- -Ddataset_name=mediacounts_hourly \
-- -Dmediacounts_data_directory=/wmf/data/wmf/mediacounts \
-- -Dstart_time=2015-01-02T00:00Z -Dstop_time=2015-01-03T00:00Z \
-- -submit -config oozie/cassandra/coord_mediarequest_top_files_daily.properties

ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION parse_media_file_url AS 'org.wikimedia.analytics.refinery.hive.GetMediaFilePropertiesUDF';

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
            referer,
            regexp_replace(base_name, '${separator}', '') file_path,
            COALESCE(media_classification, 'all-media-types') media_classification,
            LPAD(year, 4, "0") as year,
            LPAD(month, 2, "0") as month,
            LPAD(day, 2, "0") as day,
            SUM(request_count) as requests
        FROM (
            SELECT
                base_name,
                COALESCE(parse_media_file_url(base_name).media_classification, 'other') media_classification,
                map("all-referers", total, "internal", referer_internal, "external", referer_external, "unknown", referer_unknown) referer_map,
                'all-agents' AS agent,
                year, month, day
            FROM ${source_table}
            where year = ${year} AND month = ${month} AND day = ${day}
        ) exploded
        LATERAL VIEW explode(referer_map) rm AS referer, request_count
        WHERE
            year = ${year}
            AND month = ${month}
            AND day = ${day}
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
                referer,
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
