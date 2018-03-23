-- Generate json formatted geowiki monthly data to be loaded in Druid
--
-- REMARK: Booleans are converted to 0/1 integers to allow
-- using them both as dimensions and metrics in druid (having
-- them as metrics means for instance counting number of
-- deleted pages)
--
-- Usage:
--     hive -f generate_json_geowiki_monthly.hql \
--         -d source_table=wmf.geowiki_monthly \
--         -d destination_directory=/tmp/druid_private/geowiki_monthly \
--         -d month=2018-01
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS `tmp_druid_geowiki_monthly`;

CREATE EXTERNAL TABLE `tmp_druid_geowiki_monthly` (
  `month`               string      COMMENT 'The partition of the data, needed for druid to have a timestamp',
  `wiki_db`             string      COMMENT 'The wiki database the editors worked in',
  `country_code`        string      COMMENT 'The 2-letter ISO country code this group of editors geolocated to, including Unknown (--)',
  `users_are_anonymous` int         COMMENT 'Whether or not this group of editors edited anonymously',
  `activity_level`      string      COMMENT 'How many edits this group of editors performed, can be "at least 1", "at least 5", or "at least 100"',
  `distinct_editors`    bigint      COMMENT 'Number of editors meeting this activity level'
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


 INSERT OVERWRITE TABLE tmp_druid_geowiki_monthly
 SELECT month,
        wiki_db,
        country_code,
        CASE WHEN users_are_anonymous THEN 1 ELSE 0 END AS users_are_anonymous,
        activity_level,
        distinct_editors

   FROM ${source_table}
  WHERE month = '${month}'
;

DROP TABLE IF EXISTS tmp_druid_geowiki_monthly;
