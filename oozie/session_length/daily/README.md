# FIXME Deprecated by hql/session_length/daily + Airflow

# Session length daily

This job computes daily session length data from session tick events.
For more information on metric semantics, methodology and implementation, see:
https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Traffic/SessionLength

## Usage examples

### Start/restart/re-run/backfill coodrinator in production
Modify start_time (and stop_time?) appropriately.
```
sudo -u analytics kerberos-run-command analytics oozie job --oozie $OOZIE_URL \
    -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/$(date +"%Y")* | tail -n 1 | awk '{print $NF}') \
    -Dqueue_name='production' \
    -Dstart_time='2021-01-01T00:00Z' \
    -config /srv/deployment/analytics/refinery/oozie/session_length/daily/coordinator.properties \
    -run
```

### Test code changes
Modify parameters to point to your test assets, and desired time interval.
Remember to set proper permissions to the destination directory so that the
analytics user can write to it.
```
sudo -u analytics kerberos-run-command analytics oozie job --oozie $OOZIE_URL \
    -Doozie_directory='hdfs://analytics-hadoop/user/mforns/oozie' \
    -Dsession_length_data_directory='hdfs://analytics-hadoop/user/mforns/data/session_length_daily' \
    -Ddestination_table='mforns.session_length_daily' \
    -Dstart_time='2021-01-01T00:00Z' \
    -Dstop_time='2021-01-22T00:00Z' \
    -Dsla_alert_contact='mforns@wikimedia.org' \
    -Dsend_error_email_workflow_file='no_email_please' \
    -config /home/mforns/refinery/oozie/session_length/daily/coordinator.properties \
    -run
```
