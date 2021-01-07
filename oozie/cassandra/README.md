# Cassandra loader for AQS

This collection of coordinators is responsible for data transformation
and loading into cassandra for Analytics Query Service.
It has these main datasets:

* Pageviews per article
* Pageviews per project
* Pageviews top articles
* Pageviews by country
* Pageviews per country
* Mediarequests per file
* Mediarequests per referer
* Mediarequests top files
* Unique devices
* Pagecounts per project (historical)
* Editors by country

First, data is written in TSV files with aggregates computed
in a cube fashion for some dimensions. Then, those flat files are
loaded into cassandra, and finally removed if the job ends correctly.

Note: Some time ago, this was a bundle that encompassed all coordinators.
But in the end this resulted to be problematic when restarting jobs,
back-filling or testing. Since then, individual coordinator property files
are provided.

Note2: This job uses checked data sets. A source data set will not be
processed until the corresponding directory has a SUCCESS flag.
