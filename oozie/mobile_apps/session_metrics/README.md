Oozie job to schedule app session metrics reports. The reports are
set to be generated every 7 days. There are 2 kinds of reports:

- Global: Considers all sessions for the last 30 days of data.
- By OS: Considers the last 7 days of data and splits the sessions
  by OS family (Android, iOS).

The oozie workflow launches a spark action that runs the
AppSessionMetrics scala job in analytics-refinery-source/refinery-job here -
https://phabricator.wikimedia.org/diffusion/ANRS/

Example command for running the bundle on command line:

    oozie job -run \
         -config bundle.properties \
         -D refinery_directory=hdfs://analytics-hadoop/user/madhuvishy/refinery \
         -D spark_job_jar=hdfs://analytics-hadoop/user/madhuvishy/source/refinery-job-0.0.12-SNAPSHOT.jar \
         -D output_directory=hdfs://analytics-hadoop/user/madhuvishy/session_metrics

The results of this job are stored at

- {output_directory}/session_metrics/session_metrics.tsv (global)
- {output_directory}/session_metrics_by_os/session_metrics.tsv (by OS)

They're also exposed as hive external tables. See:

- refinery/hive/mobile_apps/create_mobile_apps_session_metrics_table.hql
- refinery/hive/mobile_apps/create_mobile_apps_session_metrics_by_os_table.hql
