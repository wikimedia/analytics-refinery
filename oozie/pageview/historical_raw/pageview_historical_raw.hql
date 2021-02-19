-- Loads a dump file into the table that holds the legacy pageview
-- data. The resulted data is reaggregated with unskewed hourly data,
-- and its project names corrected to the standard wiki domain form.
--
--
-- Parameters:
--     refinery_hive_jar_path -- the jar needed to load the UDFs from
--     source_dump_table      -- the table to temporarily put the dump data in
--     source_dump_location   -- the location of the temporary dump table
--     source_dump_path       -- the path to the dump file itself
--     destination_table      -- the table where hourly values are loaded into
--     year
--     month
--     day
--     hour
--
--
-- Usage:
-- hive -f oozie/pageview/historical/pageview_historical/pageview_historical.hql \
-- -d refinery_hive_jar_path=hdfs://analytics-hadoop/user/fdans/refhive.jar \
-- -d source_dump_table=pagecounts_ez.tmp_raw_2020_01_01_00 \
-- -d source_dump_location=hdfs://analytics-hadoop/user/fdans/rawdumps/2019/2019-12 \
-- -d source_dump_path=hdfs://analytics-hadoop/user/fdans/rawdumps/2020/2020-01/pagecounts-2019-12-31-230000.bz2 \
-- -d destination_table=wmf.pageview_historical \
-- -d year=2020 \
-- -d month=1 \
-- -d day=1 \
-- -d hour=0


ADD JAR ${refinery_hive_jar_path};

-- Step 1: create temporary table to store the raw dump
DROP TABLE IF EXISTS ${source_dump_table};
set hive.exec.compress.output = true;
set mapred.output.compression.codec= org.apache.hadoop.io.compress.GzipCodec;
CREATE EXTERNAL TABLE ${source_dump_table} (
    project_shorthand string,
    page_title string,
    views bigint
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${source_dump_location}';

-- Step 2: load temporary table with dump file.
LOAD DATA INPATH
'${source_dump_path}'
INTO TABLE ${source_dump_table};


-- Step 3: convert the project from the old format to standard url format,
-- and partition by hour, setting page_id as null.
CREATE TEMPORARY FUNCTION ez_wiki_to_standard AS 'org.wikimedia.analytics.refinery.hive.ConvertEZProjectToStandard';
INSERT OVERWRITE TABLE ${destination_table}
PARTITION (year=${year}, month=${month}, day=${day}, hour=${hour})
SELECT
    ez_wiki_to_standard(project_shorthand) project,
    page_title,
    'user' agent_type,
    'desktop' access_method,
    NULL AS page_id,
    views
FROM ${source_dump_table}
;

DROP TABLE ${source_dump_table};
