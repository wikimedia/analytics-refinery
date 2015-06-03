Oozie job to schedule app session metrics reports. The reports are
set to be generated every 7 days for the last 30 days of data.

The oozie workflow launches a spark action that runs the
AppSessionMetrics scala job in analytics-refinery-source/refinery-job here -
https://phabricator.wikimedia.org/diffusion/ANRS/

Example command for running the coordinator on command line:

    oozie job -run \
         -config coordinator.properties \
         -D refinery_directory=hdfs://analytics-hadoop/user/madhuvishy/refinery \
         -D spark_job_jar=hdfs://analytics-hadoop/user/madhuvishy/source/refinery-job-0.0.12-SNAPSHOT.jar \
         -D output_directory=hdfs://analytics-hadoop/user/madhuvishy/session_metrics

The results of this job is stored at output_directory/session_metrics.tsv. It's also exposed as
a hive external table - see refinery/hive/mobile_apps/create_mobile_apps_session_metrics_table.hql