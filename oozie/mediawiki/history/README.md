Collection of jobs processing Mediawiki History:
 - load  -- Add hive partitions for sqooped raw_data
 - denormalize  -- run spark job rebuilding a denormalized version of history
   from imported raw data and finally add hive patitions
   for those (for a single snapshot).
 - metrics  -- Multiple Hive queries generating metrics out of the
   denormalized dataset. Updates a hive table with results.
 - druid  -- Load the newly generated denormalized data into druid (druid is
   setup to only keep the last 2 years of this data, but having it indexed
   is interested in case we'd need to load more). The druid job only indexes
   labs data.