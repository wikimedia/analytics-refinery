# Feature calculation phase from webrequest data
# initially used for automated traffic detection

This job is responsible for filtering pageview data
from the refined webrequest table, aggregating it into
"actors" (or pseudo-sessions)
and calculating features per actor

Output is appended into (year, month, day, hour) partitions
in /wmf/data/learning/features/actor/hourly

Note that this job uses the refined dataset. If a webrequest refinement job
does not have the _SUCCESS done-flag in the directory, the data for that
hour will not be aggregated until it does.
