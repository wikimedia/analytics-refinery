Oozie job to schedule app session metrics report. The report is
set to be generated every 7 days, considers the last 7 days of data
and splits the sessions by OS family (Android, iOS).

The oozie workflow launches a spark action that runs the
AppSessionMetrics scala job in analytics-refinery-source/refinery-job here -
https://phabricator.wikimedia.org/diffusion/ANRS/

Example command for running the coordinator on command line:

    oozie job -run \
         -config coordinator.properties \
         -D refinery_directory=hdfs://analytics-hadoop/user/madhuvishy/refinery \
         -D spark_job_jar=hdfs://analytics-hadoop/user/madhuvishy/source/refinery-job-X.X.X.jar \
         -D output_directory=hdfs://analytics-hadoop/user/madhuvishy/session_metrics/session_metrics_by_os

The result of this job are stored at

- {output_directory}/session_metrics.tsv (by OS)

It is also exposed as hive external table. See:

- refinery/hive/mobile_apps/create_mobile_apps_session_metrics_by_os_table.hql
