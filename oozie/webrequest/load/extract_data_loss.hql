-- Writes a line in a file if data loss in webrequest statistics
-- hourly for a given source and hour is above given threshold
--
-- The file with the line is ${target}/000000_0
-- If data loss is below threshold, the file will exist, but will be
-- empty.
--
-- Parameters:
--     table             -- Fully qualified table name containing the
--                          hourly statistics to read.
--     target            -- Path in HDFS where to write the file.
--                          If this path exists, it will get overwritten.
--     threshold         -- The threshold for data loss not to be above.
--     webrequest_source -- webrequest_source for the partition to
--                          chek data loss for.
--     year              -- year for the partition to chek data loss for.
--     month             -- month for the partition to chek data loss for.
--     day               -- day for the partition to chek data loss for.
--     hour              -- hour for the partition to chek data loss for.
--
-- Usage:
--     hive -f extract_data_loss.hql \
--         -d table=wmf_raw.webrequest_sequence_stats_hourly \
--         -d target=hdfs:///tmp/data_loss_check \
--         -d threshold=1 \
--         -d webrequest_source=bits \
--         -d year=2016 \
--         -d month=1 \
--         -d day=1 \
--         -d hour=0
--


-- Hard-limiting number of reducer to force a single file in the
-- target directory.
SET mapred.reduce.tasks=1;

-- Allow INSERT OVERWRITE into nested directory, so we need not take
-- care of creating directories
SET hive.insert.into.multilevel.dirs=true;


INSERT OVERWRITE DIRECTORY '${target}'
-- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY '	'" only
-- works for exports to local directories (see HIVE-5672), we have to
-- prepare the lines by hand through concatenation :-(
  SELECT
    CONCAT(
      CAST(count_lost AS string),
      ' lost events generate ',
      CAST(percent_lost AS string),
      '% loss, above ${threshold}.'
    ) line
    FROM ${table}
    WHERE
        percent_lost > ${threshold} AND
        webrequest_source='${webrequest_source}' AND
        year=${year} AND month=${month} AND day=${day} AND hour=${hour}
;
