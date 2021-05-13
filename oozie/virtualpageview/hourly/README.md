# Aggregation from event.virtualpageview to wmf.virtualpageview_hourly.

This job is responsible for aggregating EP virtualpageview events
into wmf.virtualpageview_hourly data set. Output is appended into
(year, month, day, hour) partitions in /wmf/data/wmf/virtualpageview/hourly.

# Outline

* ```coordinator.properties``` is used to define parameters to the aggregation pipeline.
* ```coordinator.xml``` injects the aggregation workflow for each dataset.
* ```workflow.xml``` runs a hive query to aggregate from virtualpageview into virtualpageview_hourly.

Note that this job uses an EL dataset. If an EL refinement job does not
have the _REFINED done-flag in the directory, the data for that hour will
not be aggregated until it does.
