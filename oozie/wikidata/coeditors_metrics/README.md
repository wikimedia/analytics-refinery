Oozie job to schedule generating metrics for Wikidata co-editors.
The job runs every month and reports metrics to Graphite.

The oozie workflow launches a spark action that runs the
Wikidata CoEditors scala job in analytics-refinery-source/refinery-job here -
https://phabricator.wikimedia.org/diffusion/ANRS/

Example command for running the coordinator on command line:

    oozie job -run \
         -config coordinator.properties \
         -D refinery_directory=hdfs://analytics-hadoop/wmf/refinery/current \
         -D spark_job_jar=hdfs://analytics-hadoop/wmf/refinery/current/artifacts/refinery-job.jar

The results of this job can be viewed in Graphite (graphite.wikimedia.org) under the
monthly.wikidata.coeditors namespace
