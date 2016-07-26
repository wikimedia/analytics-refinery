This directory contains the dataset definition and coordinators that launch
jobs specific to data loaded from MediaWiki's Avro+Kafka data pipeline.

If you are producing a new Avro dataset via Mediawiki Monolog and Kafka,
you should use these Oozie configs to import your data and automatically add Hive partitions to it.  Most things needed to do this are abstracted here via the 'channel' property that is distinct for each coordinator launched by bundle.xml.

Steps to add a new coordinator:

- Add a CREATE TABLE hive file in hive/mediawiki and create your table in Hive.
- Add a new coordinator declaration in bundle.xml and set $channel
  and $raw_data_directory appropriately.
- Relaunch the bundle.
