# Browser usage

This job computes weekly browser usage reports from the table:
pageview_hourly. It generates 2 TSV datasets: mobile report, and
desktop+mobile report.

Output is archived in the folders:
```archive/browser/general/mobile_web-{year}-{month}-{day}``` and
```archive/browser/general/desktop_and_mobile_web-{year}-{month}-{day}```

# Outline

* ```browser_general.hql``` is the hive query that collects the
  data from the tables, aggregates it, and writes the reports in
  the given destination directory. It is actually a template with
  some dynamic parameters: input and output paths, time info and
  a filter for access methods to include in the report.

* ```workflow.xml``` declares the actions that oozie will take
  when calling the hive query. In this case, it executes the
  hive query twice, once for mobile and once for desktop+mobile.
  It also sets up some oozie-specific configuration parameters.

* ```coordinator.xml``` determines when the workflow shoud be
  executed and on which frequency. In this case, it depends on
  2 datasets: projectviews and pageviews. Both of them need a
  full week of data, starting on Sunday.

* ```coordinator.properties``` defines the default parameters
  for the pipeline. They will be passed implicitly or explicitly
  down the coordinator, the workflow and the query.
