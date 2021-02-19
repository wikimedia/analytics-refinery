-- Creates a temporary table containing daily pageview data,
-- each row containing hourly values encoded in Pagecounts-EZ
-- format. The table is partitioned by agent_type and dumps files
-- will be copied from here.
--
-- Parameters:
--     source_table             -- Fully qualified table name to get pageviews from.
--     temporary_table          -- Fully qualified table name to dump results to before creating files.
--     destination_directory    -- Where to put the generated files
--     year
--     month
--     day
--
-- Usage:
-- hive -f oozie/pageview/daily_dump/make_dumps.hql \
-- -d source_table=wmf.pageview_hourly \
-- -d temporary_table=tmp-pageviews-2020-01-01
-- -d destination_directory=hdfs://analytics-hadoop/user/fdans/pageviewdumptest \
-- -d year=2020 \
-- -d month=5 \
-- -d day=1

SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.BZip2Codec;

-- We use a temporary table instead of writing directly to an
-- HDFS folder to take advantage of the dynamic partitioning and
-- write 3 folders at once without having to go over the data multiple times.
-- The single file for each agent type is made thanks to the ORDER BY statement
-- at the bottom.
DROP TABLE IF EXISTS ${temporary_table};
CREATE EXTERNAL TABLE ${temporary_table} (
    line string
)
PARTITIONED BY (agent_type string)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${destination_directory}'
;

INSERT OVERWRITE TABLE ${temporary_table}
PARTITION (agent_type)
SELECT
    CONCAT_WS(' ',
        project,
        page_title,
        COALESCE(CAST(page_id AS STRING), 'null'),
        REGEXP_REPLACE(access_method, ' ', '-'),
        CAST(SUM(view_count) AS STRING),
        REGEXP_REPLACE(CONCAT(
            CONCAT('A', CAST(SUM(IF(hour=0, view_count, 0)) AS STRING)),
            CONCAT('B', CAST(SUM(IF(hour=1, view_count, 0)) AS STRING)),
            CONCAT('C', CAST(SUM(IF(hour=2, view_count, 0)) AS STRING)),
            CONCAT('D', CAST(SUM(IF(hour=3, view_count, 0)) AS STRING)),
            CONCAT('E', CAST(SUM(IF(hour=4, view_count, 0)) AS STRING)),
            CONCAT('F', CAST(SUM(IF(hour=5, view_count, 0)) AS STRING)),
            CONCAT('G', CAST(SUM(IF(hour=6, view_count, 0)) AS STRING)),
            CONCAT('H', CAST(SUM(IF(hour=7, view_count, 0)) AS STRING)),
            CONCAT('I', CAST(SUM(IF(hour=8, view_count, 0)) AS STRING)),
            CONCAT('J', CAST(SUM(IF(hour=9, view_count, 0)) AS STRING)),
            CONCAT('K', CAST(SUM(IF(hour=10, view_count, 0)) AS STRING)),
            CONCAT('L', CAST(SUM(IF(hour=11, view_count, 0)) AS STRING)),
            CONCAT('M', CAST(SUM(IF(hour=12, view_count, 0)) AS STRING)),
            CONCAT('N', CAST(SUM(IF(hour=13, view_count, 0)) AS STRING)),
            CONCAT('O', CAST(SUM(IF(hour=14, view_count, 0)) AS STRING)),
            CONCAT('P', CAST(SUM(IF(hour=15, view_count, 0)) AS STRING)),
            CONCAT('Q', CAST(SUM(IF(hour=16, view_count, 0)) AS STRING)),
            CONCAT('R', CAST(SUM(IF(hour=17, view_count, 0)) AS STRING)),
            CONCAT('S', CAST(SUM(IF(hour=18, view_count, 0)) AS STRING)),
            CONCAT('T', CAST(SUM(IF(hour=19, view_count, 0)) AS STRING)),
            CONCAT('U', CAST(SUM(IF(hour=20, view_count, 0)) AS STRING)),
            CONCAT('V', CAST(SUM(IF(hour=21, view_count, 0)) AS STRING)),
            CONCAT('W', CAST(SUM(IF(hour=22, view_count, 0)) AS STRING)),
            CONCAT('X', CAST(SUM(IF(hour=23, view_count, 0)) AS STRING))
        ), '[A-Z]0', '')
    ) AS line,
    agent_type -- Not real data, written only for partition
FROM ${source_table}
WHERE
    year=${year}
    AND month=${month}
    AND day=${day}
GROUP BY project, page_title, COALESCE(CAST(page_id AS STRING), 'null'), REGEXP_REPLACE(access_method, ' ', '-'), agent_type, day
ORDER BY line
LIMIT 1000000000
;

-- Dropping temporary table, but since it's external, the files are kept.
DROP TABLE ${temporary_table};