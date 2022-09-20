-- TO LAUNCH MONTHLY PER REFERER MEDIAREQUESTs BACKFILL
--
-- oozie job --oozie $OOZIE_URL \
-- -Duser=$USER \
-- -Dcassandra_keyspace=local_group_default_T_mediareq_per_referer_TEST \
-- -Dhive_script=../historical/monthly/backfill_mediarequest_per_referer.hql \
-- -Drefinery_hive_jar_path=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/org/wikimedia/analytics/refinery/refinery-hive-X.X.X.jar \
-- -Dsource_table=wmf.mediacounts \
-- -Ddatasets_file=hdfs://analytics-hadoop/wmf/refinery/current/oozie/mediacounts/datasets.xml \
-- -Ddataset_name=mediacounts_hourly \
-- -Dmediacounts_data_directory=/wmf/data/wmf/mediacounts \
-- \
-- -Dstart_time=2019-07-01T00:00Z -Dstop_time=2019-08-01T00:00Z \
-- \
-- -submit -config oozie/cassandra/coord_mediarequest_per_referer_monthly.properties

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
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), "0100") ts
    FROM ${source_table}
    where year = ${year} AND month = ${month}
) exploded
LATERAL VIEW explode(referer_map) rm AS referer, request_count
GROUP BY referer, media_classification, ts
GROUPING SETS (
    (referer, media_classification, ts),
    (referer, ts)
);
