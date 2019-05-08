Contains jobs computing per-domain and per-project-family unique devices
based on last-access cookies.
Also contains jobs to load druid with hive-computed values.


Per-project-family jobs are currently launched as:

```
# Daily computation
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2017* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config per_project_family/daily/coordinator.properties \
  -run

# Monthly computation
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2017* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config per_project_family/monthly/coordinator.properties \
  -run

# Daily druid loading
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2017* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config per_project_family/druid/daily/coordinator.properties \
  -run
```

Per-domain jobs are currently launched as:

```
# Daily computation
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2017* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config per_domain/daily/coordinator.properties \
  -run

# Monthly computation
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2017* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config per_domain/monthly/coordinator.properties \
  -run

# Daily druid loading
sudo -u analytics oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2017* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config per_domain/druid/daily/coordinator.properties \
  -run
```