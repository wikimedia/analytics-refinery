-- Adds a partition with location to a table.
--
-- NOTE: Since ALTER TABLE does not handle fully qualified table names
-- (HIVE-2584 [1]), we have to require database an table as separate
-- parameters.
--
-- [1] https://issues.apache.org/jira/browse/HIVE-2584
--
-- Parameters:
--     database        -- Database for ${table}.
--     table           -- Unqualified table name to add partition to.
--     partition_spec  -- Specification for a partition.
--                        E.g.: year=2014,month=5,day=12,hour=1
--     location        -- HDFS Directory containing the partitions
--                        data files.
--
-- Usage:
--     hive -f add_partition.hql \
--         -d database=wmf_raw \
--         -d table=webrequest \
--         -d partition_spec=year=2014,month=5,day=12,hour=1 \
--         -d location=hdfs:///path/to/data
--

USE ${database};
ALTER TABLE ${table}
    ADD IF NOT EXISTS
    PARTITION (${partition_spec})
    LOCATION '${location}'
;
