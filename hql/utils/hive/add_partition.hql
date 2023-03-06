-- Adds a partition with location to a table.
--
--
--
-- Parameters:
--     table           -- Unqualified table name to add partition to.
--                        E.g.: table=wmf_raw.webrequest
--     partition_spec  -- Specification for a partition.
--                        E.g.: year=2014,month=5,day=12,hour=1
--     location        -- HDFS Directory containing the partitions
--                        data files.
--
-- Usage:
--     spark3-sql -f add_partition.hql \
--         -d table=wmf_raw.webrequest \
--         -d "partition_spec=year=2014,month=5,day=12,hour=1" \
--         -d location=hdfs:///path/to/data
--

ALTER TABLE ${table}
    ADD IF NOT EXISTS
    PARTITION (${partition_spec})
    LOCATION '${location}'
;
