Oozie job to schedule generating varnish-requests-count for RESTBase and MW-API.
The job runs every hour and reports metrics to Graphite.

The oozie workflow launches a spark action that runs the
APIsVarnishRequests scala job in analytics-refinery-source/refinery-job

Example command for running the coordinator on command line:

    oozie job -run \
         -config coordinator.properties \
         -D refinery_directory=hdfs://analytics-hadoop/wmf/refinery/current

The results of this job can be viewed in Graphite (graphite.wikimedia.org) under
the restbase.requests and MediaWikiki.api namespaces.
