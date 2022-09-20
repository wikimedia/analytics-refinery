-- TO LAUNCH DAILY PER FILE MEDIAREQUESTS BACKFILL
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
-- -submit -config oozie/cassandra/coord_mediarequest_per_file_daily.properties

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '${separator}'
SELECT
    referer,
    regexp_replace(base_name, '${separator}', '') file_path,
    ts,
    0 AS spider,
    sum(request_count) AS agent
FROM (
    SELECT
        base_name,
        map("all-referers", total, "internal", referer_internal, "external", referer_external, "unknown", referer_unknown) referer_map,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"), "00") ts
    FROM ${source_table}
    where year = ${year} AND month = ${month} AND day = ${day}
) exploded
LATERAL VIEW explode(referer_map) rm AS referer, request_count
GROUP BY referer, regexp_replace(base_name, '${separator}', ''), ts;
