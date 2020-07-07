# Aggregation phase for pageview from pageview_actor
# and archive into legacy format.

This job is responsible for filtering pageview data
from the pageview_actor table and aggregating it
into interesting dimensions, to finally
transform and archive it into legacy format.

Output is appended into (year, month, day, hour) partitions
in /wmf/data/wmf/pageview/hourly, and then archived into
/wmf/data/archive/pageview/legacy/hourly

# Outline

* ```coordinator.properties``` is used to define parameters to the
  aggregation pipeline.
* ```coordinator.xml``` injects the aggregation workflow for each dataset.
* ```workflow.xml```
  * Runs a hive query to aggregate from webrequest into pageview

Note that this job uses the pageview_actor dataset.  If one of those
job does not have the _SUCCESS done-flag in the directory, the data for that
hour will not be aggregated until it does.
