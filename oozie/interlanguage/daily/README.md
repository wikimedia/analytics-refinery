# Aggregate cross-wiki navigation counts using HTTP referrer

This job aggregates pageview_actor records into counts of navigation by users
from one wiki project to another.  Only counting desktop site browsing.

Output is appended into (yyyy-mm-dd) daily partitions
in /wmf/data/wmf/interlanguage/navigation/daily
