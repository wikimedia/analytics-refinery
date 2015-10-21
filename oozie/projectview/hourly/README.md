# Aggregation phase for projectview from pageview
# and archive into legacy format.

This job is responsible for aggregating projectview
from pageview, and then transform/archive this data
into legacy format.

Output is appended into (year, month, day, hour) partitions
in /wmf/data/wmf/projectview/hourly, and then archived into
/wmf/data/archive/projectview/legacy/hourly

# Outline

* ```coordinator.properties``` is used to define parameters to the
  aggregation pipeline.
* ```coordinator.xml``` injects the aggregation workflow for each dataset.
* ```workflow.xml```
  * Runs a hive query to aggregate from pageview into projectview

Note that this job uses the pageview dataset.  If a pageview aggregation job
does not have the _SUCCESS done-flag in the directory, the data for that
hour will not be aggregated until it does.
