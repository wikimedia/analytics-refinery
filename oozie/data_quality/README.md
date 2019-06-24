# Data quality

This collection of jobs extracts data quality metrics from various hive tables
and writes them to the data quality tables.

## Bundles
There is one bundle per granularity. The possible granularities are: hourly,
daily and monthly. Note that some granularities might not be implemented yet
**(TODO)**. The reason we have separate bundles is so we can start them
independently, otherwise lots of unnecessary reruns would happen.

#### hourly
This bundle generates data quality metrics for data sets that are organized
in hourly partitions. The generated metrics will be stored in the hourly data
quality table.

#### daily
This bundle generates data quality metrics for data sets that are organized
in daily partitions. The generated metrics will be stored in the daily data
quality table.

#### monthly
This bundle generates data quality metrics for data sets that are organized
in monthly partitions. The generated metrics will be stored in the monthly
data quality table. This granularity can also be used for snapshot data sets.

#### queries folder
Within each bundle folder there's a queries folder. It contains the *.hql*
files responsible for extracting data quality metrics and inserting them into
the data quality table.

## How to add a new metric

#### 1) Choose a granularity
Choose the granularity that suits best your source data set. From now on you'll
be working on the corresponding granularity bundle folder. If the granularity
that you need is still not implemented... You can add it! :] If you do so,
don't forget to set up automatic deletion of old data quality records, i.e. by
using refinery/bin/refinery-drop-older-than.

#### 2) Add a new query
Create a new file in the queries folder within your chosen bundle folder.

Use INSERT OVERWRITE so that we can re-run the job and overwrite previous
results instead of adding to them. Also partition the output with 6 fields,
for example:
```
INSERT OVERWRITE TABLE ${destination_table} PARTITION (
    source_table = '${source_table}',
    query_name = '<your_query_name>',
    year = ${year},
    month = ${month},
    day = ${day},
    hour = ${hour}
)
```
Then, the SELECT statement must generate 2 fields: *metric* (string) and
*value* (double). Those should contain the data quality metric value for the
specified time range. In the WHERE clause, remember to pass the appropriate
amount of time partitions depending on your bundle's granularity. Note you
can add several metrics with a single query by using UNION ALL. For example:
```
SELECT
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
UDF; *source_table* and *destination_table* for ease of testing.

#### 3) Register a new coordinator
Add a coordinator snippet to your *bundle.xml* file, for instance:
```
<coordinator name="YOUR_COORD_NAME">
    <app-path>${coordinator_file}</app-path>
    <configuration>
        <property><name>source_table</name><value>YOUR_SOURCE_TABLE</value></property>
        <property><name>source_directory</name><value>YOUR_SOURCE_DIRECTORY</value></property>
        <property><name>query_name</name><value>YOUR_QUERY_NAME</value></property>
    </configuration>
</coordinator>
```

*YOUR_COORD_NAME* has a convention. Suppose your source table is
`wmf.edit_hourly`, your query name is `edit_quality` and your granularity is
`hourly`; then your coord name should be
`data-quality-wmf.edit_hourly-edit_quality-hourly-coord`.

*YOUR_SOURCE_TABLE* is the fully qualified table name of your source data set.
For example `wmf.edit_hourly`.

*YOUR_SOURCE_DIRECTORY* is the HDFS data path of the source data set. For
instance `/wmf/data/wmf/edit_hourly`.

*YOUR_QUERY_NAME* is the name of your query file without the *.hql* extension.
For example `edit_quality`.

## Administration

Note that there is only one bundle.properties file that serves all bundles.
Because of that, you always have to specify the *granularity* argument, which
will select the corresponding bundle.

#### Run in production
```
sudo -u analytics oozie job --oozie $OOZIE_URL \
    -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2019* | tail -n 1 | awk '{print $NF}') \
    -Dqueue='production' \
    -Dgranularity='hourly' \
    -Dstart_time='2019-04-01T00:00Z' \
    -config /home/mforns/refinery/oozie/data_quality/bundle.properties \
    -run
```

#### Backfill production
```
sudo -u analytics oozie job --oozie $OOZIE_URL \
    -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2019* | tail -n 1 | awk '{print $NF}') \
    -Dqueue='production' \
    -Dgranularity='hourly' \
    -Dstart_time='2019-04-01T00:00Z' \
    -Dstop_time='2019-04-01T23:59Z' \
    -config /home/mforns/refinery/oozie/data_quality/bundle.properties \
    -run
```

#### Test code changes
```
sudo -u analytics oozie job --oozie $OOZIE_URL \
    -Dgranularity='hourly' \
    -Dstart_time='2019-04-01T00:00Z' \
    -Dstop_time='2019-04-01T23:59Z' \
    -Dartifacts_directory='hdfs://analytics-hadoop/user/mforns/artifacts' \
    -Doozie_directory='hdfs://analytics-hadoop/user/mforns/oozie' \
    -Drefinery_jar_version='0.0.92-SNAPSHOT' \
    -Ddata_quality_table='mforns.data_quality_hourly' \
    -Ddata_quality_directory='hdfs://analytics-hadoop/user/mforns/data_quality/hourly' \
    -config /home/mforns/refinery/oozie/data_quality/bundle.properties \
    -run
```
