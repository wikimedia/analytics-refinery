Oozie jobs to schedule importing virtualpageviews in druid.
Daily and monthly folders contain coordinators to load in druid:
 - Each day, virtualpageviews at hourly granularity (daily folder).
 - Each month, virtualpageviews at hourly granularity (monthly folder).

The workflow launches a hive action generating json data,
then launches the druid indexation on this files.
The script waits for indexation to finish then delete the json files.

Example command for running the coordinator on command line:

    oozie job -run \
         -config daily/coordinator.properties \
         -D refinery_directory=hdfs://analytics-hadoop/wmf/refinery/current
