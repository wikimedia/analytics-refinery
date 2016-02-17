# Browser usage

This job computes weekly browser usage stats from pageview_hourly.
The results serve as an intermediate table for various traffic reports,
i.e.: mobile web browser breakdown, desktop os breakdown, or
desktop+mobile web os+browser breakdown, etc. Output is stored in
the table: wmf.browser_general

# Outline

* ```browser_general.hql``` is the hive query that collects the
  data from the pageview_hourly, aggregates it, and writes the
  results into the given destination table. It is actually a
  template with some dynamic parameters passed in by the workflow.

* ```workflow.xml``` sets up some oozie-specific configuration
  parameters, and calls the hql query, passing in the necessary
  parameters. Also defines what to do in case of query failure.

* ```coordinator.xml``` determines when the workflow shoud be
  executed and on which frequency. In this case, it depends on
  2 datasets: projectviews and pageviews. Both of them need a
  full day of data, starting on Sunday.

* ```coordinator.properties``` defines the default parameters
  for the pipeline. They will be passed implicitly or explicitly
  down the coordinator, the workflow and the query.
