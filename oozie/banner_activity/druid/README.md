Oozie jobs to schedule importing banner activity data into druid.
Daily and monthly folders contain coordinators to load in druid:
 - Each day, banner activity at minute detail (daily folder)
 - Each month, banner activity at same minute detail (monthly folder)
Note that both jobs overwrite the same druid dataset.

The workflow launches a hive action generating json data,
then launches the druid indexation on this files.
The script waits for indexation to finish then delete the json files.

Example command for running the coordinator on command line:

    oozie job -run \
        -config daily/coordinator.properties \
        -D refinery_directory=hdfs://analytics-hadoop/wmf/refinery/current \
        -Dstart_time=2017-01-01T00:00Z \
        -Dstop_time=2017-01-02T00:00Z
