Oozie jobs to schedule importing banner activity data into druid.
Daily and monthly folders contain coordinators to load in druid:
 - Each day, banner activity at minute detail (daily segments).
 - Each month, sanitized and compacted banner activity also at
   minutely detail (monthly segments). Note: this job is executed
   with a 3 months lag.
Note that both jobs overwrite the same druid dataset.

Daily, the workflow launches a hive action generating json data,
then launches the druid indexation on this files. Finally the script
deletes the json folder once indexation finishes.

Monthly, only a druid indexation happens, reloading data over itself.
The druid template uses the existing datasource filled in with data
and compresses it into better shaped indexes (monthly segments).
Also, sanitizes the banner activity data set to not contain any
privacy-sensitive fields.
Note: this job needs to use a hack: reusing the druid loading
subworkflow makes it mandatory to have a directory_source being marked
as valid (containing a _SUCCESS file). Since this job doesn't need a
directory source (it's using already indexed data as source), we use
a webrequest hour folder as directory source (it's never use, except
for making the job start).
Note that the monthly job waits for 3 months before executing, thus
making it easier to re-run daily jobs if necessary and also delaying
the sanitization until the 60-90 days specified by the data retention
guidelines.

Example command for running the coordinator on command line:

    oozie job -run \
        -config daily/coordinator.properties \
        -D refinery_directory=hdfs://analytics-hadoop/wmf/refinery/current \
        -Dstart_time=2017-01-01T00:00Z \
        -Dstop_time=2017-01-02T00:00Z


The `druid_kafka_supervisor_example.json` file is an example configuration
launching a druid-kafka-supervisor task - a realtime ingestion task from kafka
data. It uses the `eventlogging_CentralNoticeImpression` kafka topic as source
and sends flattened and transformed data to the
`test_kafka_event_centralnoticeimpression` druid datasource.
To start the supervisor you'd use the following command from a stat100X machine:
```
curl -L -X POST -H 'Content-Type: application/json' \
  -d @druid_kafka_supervisor_example.json \
  http://an-druid1001.eqiad.wmnet:8090/druid/indexer/v1/supervisor
```
