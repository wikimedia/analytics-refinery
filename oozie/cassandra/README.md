# Cassandra loader for AQS

The bundle is responsible for data transformation and load
into cassandra for the Analytics Query Service. It has these main datasets:
* Pageviews per article
* Pageviews per project
* Pageviews top articles
* Unique devices
* Historical pagecounts per project
* Mediarequests per file
* Mediarequests aggregated per referer
* Top files by mediarequests

First data is written in TSV files with aggregates computed
in a cube fashion for some dimensions, then those flat files are
loaded into cassandra, and fianlly removed if the job ends correctly.

Individual coordinators properties files are also provided
to ease backfill / testing.

# Outline

* ```bundle.properties``` is used to inject the whole cassandra feeder
  pipeline into oozie.
* ```bundle.xml``` injects separate coordinators for each of the
  datasets.
* ```[hourly|daily]/coordinator.xml``` injects a workflow for each dataset.
* ```[hourly|daily]/workflow.xml```
  * Runs the hive query to aggregate certain dimensions.
  * Runs a hadoop job to load data into cassandra.
  * Removes temporary files

Note that this job uses the checked dataset.  If a needed dataset
does not have the _SUCCESS done-flag in the directory, the data for that
hour will not be worked until it does.
