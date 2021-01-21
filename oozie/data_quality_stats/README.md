# Data quality stats

This collection of jobs extracts data quality stats from various hive tables
and writes them to the data quality stats table. Then applies an anomaly
detection algorithm based on RSVD to them and sends alerts whenever they are
anomalous.

## Bundles
There is one bundle per granularity. The possible granularities are: hourly,
daily and monthly. Note that some granularities might not be implemented yet
**(TODO)**. The reason we have separate bundles is so we can start them
independently, otherwise lots of unnecessary reruns would happen.

#### hourly
This bundle generates data quality stats for data sets that are organized in
hourly partitions. The generated stats will be stored within the partition
granularity=hourly. Anomaly detection and alerting will be done hourly.

#### daily
This bundle generates data quality stats for data sets that are organized in
daily partitions. The generated stats will be stored within the partition
granularity=daily.  Anomaly detection and alerting will be done daily.

#### monthly
This bundle generates data quality stats for data sets that are organized in
monthly partitions. The generated stats will be stored within the partition
granularity=monthly. Anomaly detection and alerting will be done monthly.
This granularity can also be used for snapshot data sets.

#### queries folder
Within each bundle folder there's a queries folder. It contains the *.hql*
files responsible for extracting data quality stats and inserting them into
the data quality stats table.

## How to add new stats

#### 1) Choose a granularity
Choose the granularity that suits best your source data set. From now on you'll
be working on the corresponding granularity bundle folder. If the granularity
that you need is still not implemented... You can add it! :]

#### 2) Add a new query
Create a new file in the queries folder within your chosen bundle folder.

Use INSERT OVERWRITE; that is the behavior expected by the data quality stats
workflow. Also partition the output with the following 3 fields:
```
INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source_table = '${source_table}',
    query_name = '<your_query_name>',
    granularity = '<your_granularity>'
)
```
Then, the SELECT statement must generate 3 fields: *dt* (string), *metric*
(string) and *value* (double). Those should contain the date in ISO format
('2019-08-27T14:00:00Z'), name and value of the data quality metric for the
specified time range. In the WHERE clause, remember to pass the appropriate
amount of time fields (year, month, day, hour) depending on your bundle's
granularity. Note you can add several metrics with a single query by using
UNION ALL. For example:
```
SELECT
    CONCAT('${year}-', LPAD(${month}, 2, '0'), '-', LPAD(${day}, 2, '0'), 'T', LPAD(${hour}, 2, '0'), ':00:00Z') AS dt,
    'bot_count' AS metric,
    SUM(count) AS value
FROM ${source_table}
WHERE
    user_type = 'bot' AND
    year = ${year} AND
    month = ${month} AND
    day = ${day} AND
    hour = ${hour}
```

The Oozie workflow will pass the following convenient parameters to the query:
*artifacts_directory* and *refinery_jar_version*, in case you want to add a
UDF; *source_table*, *destination_table* for ease of testing, and the time
fields corresponding to the bundle's granularity.

#### 3) Register a new coordinator
Once you've tested the query independently, add a coordinator snippet to your
*bundle.xml* file, for instance:
```
<coordinator name="YOUR_COORD_NAME">
    <app-path>${coordinator_file}</app-path>
    <configuration>
        <property><name>source_table</name><value>YOUR_SOURCE_TABLE</value></property>
        <property><name>source_directory</name><value>YOUR_SOURCE_DIRECTORY</value></property>
        <property><name>source_done_flag</name><value>YOUR_DONE_FLAG</value></property>
        <property><name>query_name</name><value>YOUR_QUERY_NAME</value></property>
        <property><name>deviation_threshold</name><value>YOUR_DEVIATION_THRESHOLD</value></property>
        <property><name>send_alerts_to</name><value>YOUR_ALERT_EMAILS</value></property>
    </configuration>
</coordinator>
```

*YOUR_COORD_NAME* has a convention. Suppose your source table is
`wmf.edit_hourly`, your query name is `edit_quality` and your granularity is
`hourly`; then your coord name should be
`data_quality_stats-wmf.edit_hourly-edit_quality-hourly-coord`.

*YOUR_SOURCE_TABLE* is the fully qualified table name of your source data set.
For example `wmf.edit_hourly`.

*YOUR_SOURCE_DIRECTORY* is the HDFS data path of the source data set. For
instance `/wmf/data/wmf/edit_hourly`.

*YOUR_DONE_FLAG* is the name of the success file of the source data set. For
example: `_SUCCESS`.

*YOUR_QUERY_NAME* is the name of your query file without the *.hql* extension.
For instance `edit_quality`.

*YOUR_DEVIATION_THRESHOLD* is the absolute normalized deviation over which
anomalies are going to be reported with alert emails. For instance `10.0`.
For more details on the definition and use of the normalized deviation, see:
https://github.com/wikimedia/analytics-refinery-source/blob/master/refinery-job/src/main/scala/org/wikimedia/analytics/refinery/job/dataquality/RSVDAnomalyDetection.scala

*YOUR_ALERT_EMAILS* is a comma-separated list of the email addresses you want
to be notified of anomalous metrics. For example `mforns@wikimedia.org`.

## Administration

Note that there is only one bundle.properties file that serves all bundles.
Because of that, you always have to specify the *granularity* argument, which
will select the corresponding bundle.

#### Run in production
```
sudo -u analytics kerberos-run-command analytics oozie job \
    --oozie $OOZIE_URL     \
    -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/$(date +"%Y")* | tail -n 1 | awk '{print $NF}') \
    -Dqueue_name='production' \
    -Dgranularity='daily' \
    -Dstart_time='2020-04-21T22:00Z' \
    -config /srv/deployment/analytics/refinery/oozie/data_quality_stats/bundle.properties \
    -run
```


#### Backfill production
To backfill we need to add a start/stop time for the dates we are backfilling to the command above
```
-Dstart_time='2020-04-01T00:00Z' \
-Dstop_time='2020-04-01T23:59Z' \
```

#### Test code changes
Have in mind that it is required to override the tmp directory
or job would default to analytics' by default

You also need to create two tables on your DB data_quality_stats and
data_quality_stats_incoming
```
oozie job --oozie $OOZIE_URL \
    -Duser='nuria' \
    -Dtemp_directory='hdfs://analytics-hadoop/tmp/nuria/' \
    -Dgranularity='daily' \
    -Dstart_time='2020-03-01T00:00Z' \
    -Dstop_time='2020-06-30T23:59Z' \
    -Doozie_directory='hdfs://analytics-hadoop/tmp/oozie-nuria/' \
    -Dupdater_spark_job_jar='hdfs://analytics-hadoop/wmf/refinery/current/artifacts/refinery-job.jar' \
    -Danomalies_spark_job_jar='hdfs://analytics-hadoop/wmf/refinery/current/artifacts/refinery-job.jar' \
    -Ddata_quality_stats_table='nuria.data_quality_stats' \
    -Ddata_quality_stats_base_path='hdfs://analytics-hadoop/user/nuria/data/data_quality_stats' \
    -Ddata_quality_stats_incoming_table='nuria.data_quality_stats_incoming' \
    -Dsla_alert_contact='nruiz@wikimedia.org' \
    -config /home/nuria/workplace/refinery/refinery_main/oozie/data_quality_stats/bundle.properties \
    -run
```
