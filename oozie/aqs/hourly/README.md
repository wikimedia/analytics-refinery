Deprecated by hql/aqs/hourly + Airflow

# Aggregation / reduction of aqs data from webrequest

This job is responsible for filtering aqs data
from the refined webrequest table and aggregating it into
statistically interesting dimensions.

Output is appended into (year, month, day, hour) partitions
in /wmf/data/wmf/aqs/hourly.

# Outline

* ```coordinator.properties``` is used to define parameters to the
  aggregation pipeline.
* ```coordinator.xml``` injects the aggregation workflow for each dataset.
* ```workflow.xml```
  * Runs a hive query to aggregate from webrequest into aqs_stats

Note that this job uses the refined dataset.  If a webrequest refinement job
does not have the _SUCCESS done-flag in the directory, the data for that
hour will not be aggregated until it does.
