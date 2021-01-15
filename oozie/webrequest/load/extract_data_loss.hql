-- Writes a line in a file if the incomplete data or data loss
-- in webrequest statistics hourly for a given source and hour
-- is above the given thresholds.
--
-- The file with the line is ${target}/000000_0
-- If both incomplete data and data loss are below thresholds,
-- the file will exist, but will be empty.
--
-- Parameters:
--     table                      -- Fully qualified table name containing the
--                                   hourly statistics to read.
--     target                     -- Path in HDFS where to write the file.
--                                   If this path exists, it will get overwritten.
--     incomplete_data_threshold  -- The threshold for incomplete data not to be above.
--     data_loss_threshold        -- The threshold for data loss not to be above.
--     webrequest_source          -- webrequest_source for the partition to
--                                   chek data loss for.
--     year                       -- year for the partition to chek data loss for.
--     month                      -- month for the partition to chek data loss for.
--     day                        -- day for the partition to chek data loss for.
--     hour                       -- hour for the partition to chek data loss for.
--
-- Usage:
--     hive -f extract_data_loss.hql \
--         -d table=wmf_raw.webrequest_sequence_stats_hourly \
--         -d target=hdfs:///wmf/tmp/analytics/data_loss_check \
--         -d incomplete_data_threshold=2 \
--         -d data_loss_threshold=1 \
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

-- Allow cartesian joins, so we can join the two CTEs (one row each).
SET hive.mapred.mode=nonstrict;

WITH
  data_loss AS (
    SELECT
      count_lost AS count,
      percent_lost AS `percent`
    FROM
      ${table}
    WHERE
      webrequest_source='${webrequest_source}' AND
      year=${year} AND month=${month} AND day=${day} AND hour=${hour}
  ),
  incomplete_data AS (
    SELECT
      count_incomplete AS count,
      ((count_incomplete / (count_actual + count_incomplete)) * 100.0) AS `percent`
    FROM
      ${table}
    WHERE
      webrequest_source='${webrequest_source}' AND
      year=${year} AND month=${month} AND day=${day} AND hour=${hour}
  )

INSERT OVERWRITE DIRECTORY '${target}'
-- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
-- works for exports to local directories (see HIVE-5672), we have to
-- prepare the lines by hand through concatenation :-(
SELECT
  CONCAT(
    CAST(incomplete_data.count AS string),
    ' requests (',
    CAST(ROUND(incomplete_data.`percent`, 3) AS string),
    '% of total) have incomplete records. ',
    CAST(data_loss.count AS string),
    ' requests (',
    CAST(ROUND(data_loss.`percent`, 3) AS string),
    '% of valid ones) were lost.'
  ) line
FROM
  data_loss, incomplete_data
WHERE
  data_loss.`percent` > ${data_loss_threshold} OR
  incomplete_data.`percent` > ${incomplete_data_threshold}
;
