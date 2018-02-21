-- Generate json formatted geowiki monthly data to be loaded in Druid
--
-- REMARK: Booleans are converted to 0/1 integers to allow
-- using them both as dimensions and metrics in druid (having
-- them as metrics means for instance counting number of
-- deleted pages)
--
-- Usage:
--     hive -f generate_json_geowiki_daily.hql \
--         -d source_table=wmf.geowiki_daily \
--         -d destination_directory=/tmp/druid_private/geowiki_daily \
--         -d month=2018-01
--

SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


ADD JAR /usr/lib/hive-hcatalog/share/hcatalog/hive-hcatalog-core.jar;


DROP TABLE IF EXISTS `tmp_druid_geowiki_daily`;

CREATE EXTERNAL TABLE `tmp_druid_geowiki_daily` (
  `wiki_db`             string      COMMENT 'The wiki database of origin',
  `country_code`        string      COMMENT 'The 2-letter ISO country code this group of edits geolocated to, including Unknown (--)',
  `user_id_or_ip`       string      COMMENT 'If an anonymous user, this is the ip of the user, otherwise it is their user id in this wiki db',
  `user_is_anonymous`   int         COMMENT 'Whether or not this user edited this group of edits anonymously',
  `date`                string      COMMENT 'The YYYY-MM-DD date for this group of edits',
  `edit_count`          bigint      COMMENT 'The total count of edits for this group of edits'
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION '${destination_directory}';


 INSERT OVERWRITE TABLE tmp_druid_geowiki_daily
 SELECT wiki_db,
        country_code,
        user_id_or_ip,
        CASE WHEN user_is_anonymous THEN 1 ELSE 0 END AS user_is_anonymous,
        date,
        edit_count

   FROM ${source_table}
  WHERE month = '${month}'
;

DROP TABLE IF EXISTS tmp_druid_geowiki_daily;
