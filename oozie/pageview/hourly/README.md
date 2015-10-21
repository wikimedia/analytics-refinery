# Aggregation phase for pageview from webrequest
# and archive into legacy format.

This job is responsible for filtering pageview data
from the refined webrequest table, aggregating it into
interesting dimensions, and finally transforming and
archiving it into legacy format.

Output is appended into (year, month, day, hour) partitions
in /wmf/data/wmf/pageview/hourly, and then archived into
/wmf/data/archive/pageview/legacy/hourly

# Outline

* ```coordinator.properties``` is used to define parameters to the
  aggregation pipeline.
* ```coordinator.xml``` injects the aggregation workflow for each dataset.
* ```workflow.xml```
  * Runs a hive query to aggregate from webrequest into pageview

Note that this job uses the refined dataset.  If a webrequest refinement job
does not have the _SUCCESS done-flag in the directory, the data for that
hour will not be aggregated until it does.
