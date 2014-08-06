# Basic verification for webrequest logs

The basic verification analyzes each log line's sequence number and
computes per host statistics. It detects holes, duplicates, and nulls
in sequence numbers. But there is no check on new hosts arriving,
hosts getting decommissioned, or on the amount of per host traffic.

If a dataset (i.e.: webrequest_source per hour) does not have
duplicates, holes, or nulls, the directory gets a ```_SUCCESS```
marker.

# Outline

* ```bundle.properties``` can be used to inject the whole verification
  pipeline into oozie.
* ```bundle.xml``` injects separate coordinators for each of the
  webrequest_sources.
* ```coordinator.xml``` injects a workflow for each dataset.
* ```workflow.xml```
  * adds a partition to a common (not intended for researcher's use)
    table (through ```oozie/util/hive/partition/add/workflow.xml```),
  * extracts the dataset's sequence statistics into a separate table
    (so the statistics are easily queryable and need not be recomputed
    when drilling in)
  * and puts per dataset information into separate files,
  * analyzes those files to determine whether or not the dataset is
    ok, and
  * finally writes the ```_SUCCESS``` marker to the dataset, if it is
    ok.

Note that we add the partition to the table before verification, and
do not drop the partition if there is an error. Hence, the table might
contain partitions that contains duplicates/holes. This is for the
ease of the developers when trying to have a look at the data. The
table is not meant for researchers.

Icinga monitoring for the ```_SUCCESS``` marker is not part of this
setup and can be found as a separate bundle, coordinator and workflow
at oozie/webrequest/partition/monitor_done_flag
