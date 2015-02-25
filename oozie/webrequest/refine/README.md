# Refine phase for webrequest logs

This job is responsible for the refine (ETL?) phase of
webrequest logs.  It currently converts the raw JSON
logs imported from Kafka into a clustered-bucketed table
stored in Parquet format.

# Outline

* ```bundle.properties``` can be used to inject the whole refine
  pipeline into oozie.
* ```bundle.xml``` injects separate coordinators for each of the
  webrequest_sources.
* ```coordinator.xml``` injects a workflow for each dataset.
* ```workflow.xml```
  * Runs a hive query to convert from JSON into the refined data.

Note that this job uses the checked dataset.  If a raw webrequest import
does not have the _SUCCESS done-flag in the directory, the data for that
hour will not be refined until it does.

Please update the record_version if you change the table content definition
and/or schema.
_
