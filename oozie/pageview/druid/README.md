Oozie jobs to schedule importing pageviews in druid.
Daily and monthly folders contain coordinator to load in druid:
 - Each day, pageviews at hour detail (daily folder)
 - Each month, pageviews at day detail (monthly folder)

The workflow launches a hive action generating json data,
then launches the druid indexation on this files.
The script waits for indexation to finish then delete the json files.

Example command for running the coordinator on command line:

    oozie job -run \
         -config daily/coordinator.properties \
         -D refinery_directory=hdfs://analytics-hadoop/wmf/refinery/current
