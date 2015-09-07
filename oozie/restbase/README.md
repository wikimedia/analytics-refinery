Oozie job to schedule generating metrics for RESTBase. The job runs every
hour and reports metrics to Graphite.

The oozie workflow launches a spark action that runs the
RESTBaseMetrics scala job in analytics-refinery-source/refinery-job here -
https://phabricator.wikimedia.org/diffusion/ANRS/

Example command for running the coordinator on command line:

    oozie job -run \
         -config coordinator.properties \
         -D refinery_directory=hdfs://analytics-hadoop/user/madhuvishy/refinery \
         -D spark_job_jar=hdfs://analytics-hadoop/user/madhuvishy/source/refinery-job-0.0.18-SNAPSHOT.jar

The results of this job can be viewed in Graphite (graphite.wikimedia.org) under the restbase namespace,
in restbase.requests.varnish_requests.