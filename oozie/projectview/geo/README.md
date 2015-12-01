# Aggregate projectviews geographically

This job is responsible for creating archives that aggregate pageviews by
the geographic origin of their requests, at the project level

Output is archived into /wmf/data/archive/projectview/geo

# Outline

* ```coordinator.properties``` define parameters for the archive job
* ```coordinator.xml``` injects the aggregation workflow for each dataset
* ```workflow.xml```
  * Runs a hive query to aggregate projectview geographically

Note that this job uses the projectview dataset.  If a projectview job
does not have the _SUCCESS done-flag in the directory, the data for that
hour will not be aggregated until it does.
