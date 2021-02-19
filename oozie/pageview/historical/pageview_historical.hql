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
--
--
-- Usage:
-- hive -f oozie/pageview/historical/pageview_historical/pageview_historical.hql \
-- -d refinery_hive_jar_path=hdfs://analytics-hadoop/user/fdans/refhive.jar \
-- -d source_dump_table=pagecounts_ez.tmp_raw_2020_01_12 \
-- -d source_dump_location=hdfs://analytics-hadoop/user/fdans/rawdumps/2020/2020-01 \
-- -d source_dump_path=hdfs://analytics-hadoop/user/fdans/rawdumps/2020/2020-01/pagecounts-2020-01-12.bz2 \
-- -d destination_table=wmf.pageview_historical \
-- -d year=2020 \
-- -d month=1 \
-- -d day=12 \


ADD JAR ${refinery_hive_jar_path};

-- Step 1: create temporary table to store the raw dump
DROP TABLE IF EXISTS ${source_dump_table};
CREATE EXTERNAL TABLE ${source_dump_table} (
    project_shorthand string,
    page_title string,
    daily bigint,
    hourly string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ' '
LINES TERMINATED BY '\n'
STORED AS TEXTFILE
LOCATION '${source_dump_location}'
-- Dumps have a header with comments that is 25 lines long:
tblproperties("skip.header.line.count"="25");


-- Step 2: load temporary table with dump file.
LOAD DATA INPATH
'${source_dump_path}'
INTO TABLE ${source_dump_table};


-- Step 3: explode day data into hour rows using the UDTF below,
-- adjusting the hours as described here: https://w.wiki/Qu7
-- and remove the raw data table when loading is done.
CREATE TEMPORARY FUNCTION ez_wiki_to_standard AS 'org.wikimedia.analytics.refinery.hive.ConvertEZProjectToStandard';
CREATE TEMPORARY FUNCTION skew_explode AS 'org.wikimedia.analytics.refinery.hive.EZSkewExplodeUDTF';
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
INSERT INTO TABLE ${destination_table}
PARTITION (year, month, day, hour)
SELECT
    ez_wiki_to_standard(project_shorthand) project,
    page_title,
    'user' agent_type,
    'desktop' access_method,
    NULL AS page_id,
    exploded_hours.views views,
    exploded_hours.year year,
    exploded_hours.month month,
    exploded_hours.day day,
    exploded_hours.hour hour
FROM ${source_dump_table}
LATERAL VIEW skew_explode(
    ${year},
    ${month},
    ${day},
    hourly
) exploded_hours
WHERE
    -- excluding mw, which corresponds to the sum of all mobile counts
    -- for a given language
    SPLIT(project_shorthand,'\\.')[1] <> 'mw' AND
    -- excluding mobile sites in this job (not in the base job). This is useful
    -- for us to make sure that the data matches current pageview_hourly data.
    SPLIT(project_shorthand,'\\.')[1] <> 'm'
;

DROP TABLE ${source_dump_table};
