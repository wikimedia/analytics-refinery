-- TODO: not sure if these two are needed
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;
--^ To work around HIVE-3296, we have SETs before any comments

-- Hard-limiting number of reducer to force a single file in the
-- target directory.
SET mapred.reduce.tasks=1;

-- Generates daily uniques for mobile apps for iOS and Android Installation
--
-- Parameters:
--     destination_directory -- Directory in HDFS where to store the generated
--                          data in.
--     source_table      -- table containing source data
--     year              -- year of the to-be-generated
--     month             -- month of the to-be-generated
--     day               -- day of the to-be-generated
--
--
-- Usage:
--     hive -f generate_daily_uniques.hql
--         -d destination_directory=/tmp/foo
--         -d source_table=wmf_raw.webrequest
--         -d year=2014
--         -d month=4
--         -d day=1
--

DROP VIEW IF EXISTS app_uuid_view_daily_${month}_${day};
CREATE VIEW app_uuid_view_daily_${month}_${day} AS
SELECT
CASE WHEN user_agent LIKE('%iPhone%') THEN 'iOS'
  ELSE 'Android' END AS platform,
parse_url(concat('http://bla.org/woo/', uri_query), 'QUERY', 'appInstallID') AS uuid
    FROM ${source_table}

    WHERE user_agent LIKE('WikipediaApp%')
        AND uri_query LIKE('%action=mobileview%')
        AND uri_query LIKE('%sections=0%')
        AND uri_query LIKE('%appInstallID%')
        AND webrequest_source IN ('mobile','text')
        AND year=${year}
        AND month=${month}
        AND day=${day};

-- Now get a count of totals per platform
INSERT OVERWRITE DIRECTORY "${destination_directory}"
-- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
-- works for exports to local directories (see HIVE-5672), we have to
-- prepare the lines by hand through concatenation :-(
SELECT CONCAT_WS(
    "        ",
    platform,
    CAST(COUNT(DISTINCT(uuid))  AS string)
    ) line
FROM app_uuid_view_daily_${month}_${day}
GROUP BY platform;
DROP VIEW IF EXISTS app_uuid_view_daily_${month}_${day};
