# Load Edit data set to Druid

This job periodically loads data from the edit_hourly hive table to Druid.

At every workflow run, this job reads the entire edit_hourly table for a given
snapshot and loads it to Druid overriding any data that could be there.

The segment granularity is monthly, to allow for better compaction, given that
we load the full history every time. And the query granularity is hourly.
Depending on performance when adding new fields, we might switch to daily.

The coordinator triggers a workflow when the SUCCESS flag is found
in the corresponding edit_hourly data set snapshot partition.

Usage:
```
# Create a new coordinator for production.
# Note that start time is not the start of data, rather the snapshot date.
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2018* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Dstart_time='2019-02-01T00:00Z' \
  -config /home/mforns/refinery/oozie/edit/druid/coordinator.properties \
  -submit

# Rerun a failed workflow.
# The start time should be the same as the failed workflow.
# The stop time should be before the next period.
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Dstart_time='2019-02-01T00:00Z' \
  -Dstop_time='2019-02-01T01:00Z' \
  -config /home/mforns/refinery/oozie/edit/druid/coordinator.properties \
  -submit

# Test changes to this job.
# This way, the oozie code in the user's hdfs directory is used,
# and the output of the job will be written to the specified datasource.
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Dstart_time='2019-02-01T00:00Z' \
  -Dstop_time='2019-02-01T01:00Z' \
  -Doozie_directory='hdfs://analytics-hadoop/user/mforns/oozie' \
  -Ddruid_datasource='test_edits_hourly' \
  -config /home/mforns/refinery/oozie/edit/druid/coordinator.properties \
  -submit
```
