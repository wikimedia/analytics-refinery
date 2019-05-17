# Edit hourly

This job gathers event_entity=revision+event_type=create events from mediawiki history,
formats them according to https://docs.google.com/document/d/1jzrE3xdyEHed4Ek5ORRedOlEeH-i111hdmG3tBTF8QU
and populates edit_hourly hive table.

At every workflow run, this job reads the entire mediawiki history for a given snapshot
and creates the corresponding snapshot partition in edit_hourly.
If that partition already exits, it overrides it.

There are no [year, month, day, hour] partitions, because the edit_hourly data is small enough.
The field ts (extracted from mediawiki_history.event_timestamp) is truncated to the hour,
and the data is aggregated hourly.

The coordinator triggers a workflow when the PARTITIONED flag is found
in the corresponding mediawiki history data set snapshot partition.
And sets a SUCCESS flag when edit_hourly is successfully populated.

Usage:
```
# Create a new coordinator for production.
sudo -u analytics oozie job --oozie $OOZIE_URL \
    -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2018* | tail -n 1 | awk '{print $NF}') \
    -Dqueue_name=production \
    -Dstart_time='2019-01-01T00:00Z' \
    -config /home/<USER>/refinery/oozie/edit/hourly/coordinator.properties \
    -submit

# Re-run a failed workflow.
# The start time should be the same as the failed workflow.
# The stop time should be before the next period.
sudo -u analytics oozie job --oozie $OOZIE_URL \
    -Dstart_time='2019-01-01T00:00Z' \
    -Dstop_time='2019-01-01T01:00Z' \
    -config /home/<USER>/refinery/oozie/edit/hourly/coordinator.properties \
    -submit

# Test changes to this job.
# This way, the oozie code in the users hdfs directory is used,
# and the output of the job will be written to a user database.
sudo -u analytics oozie job --oozie $OOZIE_URL \
    -Dstart_time='2019-01-01T00:00Z' \
    -Dstop_time='2019-01-01T01:00Z' \
    -Doozie_directory='hdfs://analytics-hadoop/user/<USER>/oozie' \
    -Dedit_data_directory='hdfs://analytics-hadoop/user/<USER>/edit' \
    -Dedit_hourly_table='<USER>.edit_hourly' \
    -config /home/<USER>/refinery/oozie/edit/hourly/coordinator.properties \
    -submit
```
