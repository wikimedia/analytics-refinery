# Verify webrequest logs and refine them

The basic verification analyzes each log line and checks that the
timestamp and potentially other critical fields are present, also
checks the request's sequence number and computes per host
statistics. It detects holes, duplicates, and nulls in sequence
numbers. But there is no check on new hosts arriving, hosts
getting decommissioned, or on the amount of per host traffic.

If a dataset (i.e.: webrequest_source per hour) does not have
missing data, duplicates, holes, or nulls, the directory gets a
```_SUCCESS``` marker.

Then data get refined, meaning converted from raw JSON
logs imported from Kafka into a clustered-bucketed table
stored in Parquet format with newly computed fields.

# Outline

* ```bundle.properties``` can be used to inject the whole pipeline into oozie.
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
  * analyzes those files to determine whether or not the raw dataset is
    ok
  * writes the ```_SUCCESS``` marker to the raw dataset, if it is ok.
  * In that case, compute the new refined partition, and
  * writes the ```_SUCCESS``` marker to the refined partition, if it is ok.

Note that we add the partition to the table before verification, and
do not drop the partition if there is an error. Hence, the table might
contain partitions that contains duplicates/holes. This is for the
ease of the developers when trying to have a look at the data. The
table is not meant for researchers.

Icinga monitoring for the ```_SUCCESS``` marker is not part of this
setup and can be found at {{Citation needed}}.

Please update the record_version if you change the refined table content
definition and/or schema.
