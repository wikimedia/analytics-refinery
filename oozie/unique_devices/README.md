Contains jobs computing per-domain and project-wide unique devices
based on last-access cookies.
Also contains jobs to load druid with hive-computed values.


Project-wide jobs are currently launched as:

```
# Daily computation
sudo -u hdfs oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2016* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config project_wide/daily/coordinator.properties \
  -run

# Monthly computation
sudo -u hdfs oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2017* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config project_wide/monthly/coordinator.properties \
  -run

# Daily druid loading
sudo -u hdfs oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2017* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config project_wide/druid/daily/coordinator.properties \
  -run
```