-- TO LAUNCH DAILY PER REFERER MEDIAREQUESTs BACKFILL
--
-- sudo -u analytics oozie job --oozie $OOZIE_URL \
-- -Doozie_directory=hdfs://analytics-hadoop/user/fdans/refinery/oozie \
-- -Dsla_alert_contact=fdans@wikimedia.org \
-- -Duser=$USER \
-- -Dcassandra_keyspace=local_group_default_T_mediareq_per_referer_TEST \
-- -Dhive_script=backfill_mediarequest_per_referer.hql \
-- -Drefinery_hive_jar_path=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/org/wikimedia/analytics/refinery/refinery-hive-X.X.X.jar \
-- -Dsource_table=wmf.mediacounts \
-- -Ddatasets_file=hdfs://analytics-hadoop/wmf/refinery/current/oozie/mediacounts/datasets.xml \
-- -Ddataset_name=mediacounts_hourly \
-- -Dmediacounts_data_directory=/wmf/data/wmf/mediacounts \
-- \
-- -Dstop_time=2015-01-03T00:00Z -Dstart_time=2015-01-02T00:00Z \
-- \
-- -submit -config oozie/cassandra/coord_mediarequest_per_referer_daily.properties

ADD JAR ${refinery_hive_jar_path};
CREATE TEMPORARY FUNCTION parse_media_file_url AS 'org.wikimedia.analytics.refinery.hive.GetMediaFilePropertiesUDF';

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '${separator}'
SELECT
    referer,
    COALESCE(media_classification, 'all-media-types'),
    'all-agents' AS agent,
    ts,
    SUM(request_count)
FROM (
    SELECT
        base_name,
        COALESCE(parse_media_file_url(base_name).media_classification, 'other') media_classification,
        map("all-referers", total, "internal", referer_internal, "external", referer_external, "unknown", referer_unknown) referer_map,
        'all-agents' AS agent,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"), "00") ts
    FROM ${source_table}
    where year = ${year} AND month = ${month} AND day = ${day}
) exploded
LATERAL VIEW explode(referer_map) rm AS referer, request_count
GROUP BY referer, media_classification, ts
GROUPING SETS (
    (referer, media_classification, ts),
    (referer, ts)
);
