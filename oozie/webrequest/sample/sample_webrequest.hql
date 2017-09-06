-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          statistics for.
--     destination_table -- Fully qualified table name to stopre the
--                          computed statistics in. This table should
--                          have schema described in [1].
--     sample_rate       -- 1/$sample_rate sampling will be done with TABLESAMPLE
--     webrequest_source -- webrequest_source of partition to compute
--                          statistics for.
--     year              -- year of partition to compute statistics
--                          for.
--     month             -- month of partition to compute statistics
--                          for.
--     day               -- day of partition to compute statistics
--                          for.
--     hour              -- hour of partition to compute statistics
--                          for.
--
-- Usage:
--     hive -f sample_webrequest.hql                              \
--         -d source_table=wmf.webrequest                         \
--         -d destination_table=archive.webrequest_sampled_1000   \
--         -d sample_rate=1000                                    \
--         -d webrequest_source=text                              \
--         -d year=2014                                           \
--         -d month=12                                            \
--         -d day=30                                              \
--         -d hour=1
--

SET parquet.compression              = SNAPPY;
SET hive.enforce.bucketing           = true;

-- This allows us to use full dynamic partitioning when inserting, meaning that the data
-- will automatically be inserted into the correct partitions in the new table.
SET hive.exec.dynamic.partition.mode=nonstrict;


-- Insert into the new table, sampling on 1 out of $sample_rate on the hostname,seqeunce bucket.
-- We use hostname,sequence, instead of rand(), because the wmf.webrequest table itself is already
    -- bucketed on hostname,sequence.
INSERT OVERWRITE TABLE ${destination_table} PARTITION (webrequest_source, year, month, day, hour)
    SELECT *
    FROM ${source_table} TABLESAMPLE(BUCKET 1 OUT OF ${sample_rate} ON hostname,sequence)
    WHERE
        webrequest_source='${webrequest_source}' AND
        year=${year} AND month=${month} AND day=${day} AND hour=${hour}
;
