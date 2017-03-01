Includes daily and monthly hive jobs to calculate the last access cookie based uniques.

These are currently launched as:

```
sudo -u hdfs oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2016* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config daily/coordinator.properties \
  -run

sudo -u hdfs oozie job --oozie $OOZIE_URL \
  -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2016* | tail -n 1 | awk '{print $NF}') \
  -Dqueue_name=production \
  -Doozie_launcher_queue_name=production \
  -Doozie_launcher_memory=2048 \
  -Dstart_time=2015-12-01T00:00Z \
  -config monthly/coordinator.properties \
  -run
```