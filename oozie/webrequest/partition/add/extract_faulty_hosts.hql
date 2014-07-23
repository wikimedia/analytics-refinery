-- Extracts obviously faulty hosts from webrequest statistics and puts
-- them into a single file.
--
-- The file with faulty hosts information is ${target}/000000_0
-- If there are no faulty hosts, the file will exist, but will be
-- empty.
--
-- Parameters:
--     table             -- Fully qualified table name containing the
--                          statistics to analyize.
--     target            -- Path in HDFS where to write the file with
--                          obviously faulty hosts in. If this path
--                          exists, it will get overwritten.
--     webrequest_source -- webrequest_source for the partition to
--                          extractfaulty hosts for.
--     year              -- year for the partition to extract faulty
--                          hosts for.
--     month             -- month for the partition to extract faulty
--                          hosts for.
--     day               -- day for the partition to extract faulty
--                          hosts for.
--     hour              -- hour for the partition to extract faulty
--                          hosts for.
--
-- Usage:
--     hive -f extract_faulty_hosts.hql \
--         -d table=wmf_raw.webrequest_sequence_stats \
--         -d target=hdfs:///tmp/faulty_hosts \
--         -d webrequest_source=bits \
--         -d year=2014 \
--         -d month=5 \
--         -d day=12 \
--         -d hour=1
--


-- Hard-limiting number of reducer to force a single file in the
-- target directory.
SET mapred.reduce.tasks=1;

-- Allow INSERT OVERWRITE into nested directory, so we need not take
-- care of creating directories
SET hive.insert.into.multilevel.dirs=true;


INSERT OVERWRITE DIRECTORY '${target}'
    SELECT * FROM ${table} WHERE
        (
                count_duplicate != 0      -- Host has duplicates
            OR
                count_different != 0      -- Host has duplicates or holes
            OR
                count_null_sequence != 0  -- Host has NULL sequence numbers
        ) AND
        webrequest_source='${webrequest_source}' AND
        year=${year} AND month=${month} AND day=${day} AND hour=${hour}
;
