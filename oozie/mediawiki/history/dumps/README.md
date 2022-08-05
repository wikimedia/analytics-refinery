
# MediaWiki History Dumps

This job reads the data from the mediawiki_history data set, rehashes it into a
convenient set of partitions, and outputs it in the form of a Bzip2 TSV dump.
These dumps are meant to be public and available for download by the community,
as a resource for researchers and data scientists/analysts around the world.

### Updates

The job executes once a month, with the release of each new mediawiki_history
snapshot. As the dumps are snapshot-based, each month the job will regenerate
the dumps for the whole mediawiki_history since 2001.

### Versioning

The output is versioned by snapshot. This means each month a new folder named
after the snapshot (YYYY-MM) will be created with the new updated version of
the dumps. Older snapshots will be kept for a couple months, and then will be
deleted.

### Partitioning

The data will be partitioned by 2 dimensions: wiki and time. This way users can
download data for a wiki (or set of wikis) of their choice. The time split is
necessary because of file size reasons. There are 3 different time partitions:
monthly, yearly and all-time. Very big wikis will be partitioned monthly, while
medium wikis will be partitioned yearly, and small wikis will be dumped in one
single file.

### File format

The chosen output file format is TSV, to reduce the size of the dumps as much
as possible. TSV format does not have meta-data like i.e. JSON format, and even
after compression, it is lighter. Also, mediawiki_history data is pretty flat,
the only nested fields are arrays of strings, which can be encoded in TSV.

The chosen compression algorithm is Bzip2, because it's a widely used free
software format and has a high compression rate. Also, one can concatenate
several Bzip2 files and treat them as a single Bzip2 file (in case users
need to do that).

### Job details

* The Spark job will follow this steps: 1) Read all data, 2) repartition it
into the specified partitions, 3) write the resulting data making sure each
partition folder contains only 1 output file, 4) Move the generated files to
their final location with prettified filenames.

* This job has an SLA configured that will send alerts if sla_miss_after_days
days have passed after the start of the snapshot, and the corresponding dumps
have not been generated. The job also sends email alerts when there's a runtime
failure.

### Administration

##### Deleting existing dump
Before any other operation, we should delete existing dump files from HDFS.
This is not strictly needed, because the job will overwrite all existing files,
but we should make sure that there are no non-overriden files left. Replace
snapshot as needed.
```
sudo -u analytics hdfs dfs -rm -r /wmf/data/archive/mediawiki/history/dumps/2019-07
```

##### Restarting the coordinator
First, we kill the existing coordinator in Hue, and then we run a new one.
Replace refinery_directory year, start_time and properties file as needed.
```
sudo -u analytics oozie job --oozie $OOZIE_URL \
    -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2019* | tail -n 1 | awk '{print $NF}') \
    -Dqueue='production' \
    -Dstart_time='2019-07-01T00:00Z' \
    -config /home/mforns/refinery/oozie/mediawiki/history/dumps/coordinator.properties \
    -run
```

##### Re-running a failed or corrupted dump
Launch a temporary oozie coordinator to process the snapshot. Replace the
refinery_directory year, start_time, stop_time and properties file as needed.
```
sudo -u analytics oozie job --oozie $OOZIE_URL \
    -Drefinery_directory=hdfs://analytics-hadoop$(hdfs dfs -ls -d /wmf/refinery/2019* | tail -n 1 | awk '{print $NF}') \
    -Dqueue='production' \
    -Dstart_time='2019-07-01T00:00Z' \
    -Dstop_time='2019-07-01T00:01Z' \
    -config /home/mforns/refinery/oozie/mediawiki/history/dumps/coordinator.properties \
    -run
```

##### Testing code changes
If you made changes to the Spark job, then compile them and put the jar file
somewhere in your HDFS home directory. If you made changes to the xml files or
the properties file, put them in an oozie directory in your HDFS home directory.
Then run the coordinator. Replace specific values and override any other
properties as needed.
```
oozie job --oozie $OOZIE_URL \
    -Dstart_time='2019-07-01T00:00Z' \
    -Dstop_time='2019-07-01T00:01Z' \
    -Doozie_directory='hdfs://analytics-hadoop/user/mforns/oozie' \
    -Dspark_job_jar='hdfs://analytics-hadoop/user/mforns/jars/refinery-job-X.X.X.jar' \
    -Doutput_base_path='hdfs://analytics-hadoop/user/mforns/mediawiki/history/dumps' \
    -config /home/mforns/refinery/oozie/mediawiki/history/dumps/coordinator.properties \
    -run
```
