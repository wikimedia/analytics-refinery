-- Creates table statement for raw mediawiki_image table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_image_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_image`(
  `img_name`            string  COMMENT 'Filename using underscores.',
  `img_size`            bigint  COMMENT 'File size in bytes.',
  `img_width`           int     COMMENT 'Image width, in pixels.',
  `img_height`          int     COMMENT 'Image height, in pixels.',
  -- Ignored field -- In commonswiki it can contain more than 10Mb of data making the sqoop process fail
  --`img_metadata`        string  COMMENT 'Serialized PHP array of the file\'s properties. 0 if the extractor fail. T155741',
  `img_bits`            int     COMMENT 'Bit-depth of GIF/PNG palette-based images (up to 8-bit). Non-palette images (JPEG/PNG/TIFF/SVG) are 0, 8, or 16. All other files default to 0.',
  `img_media_type`      string  COMMENT 'Possibilities are: UNKNOWN, BITMAP, DRAWING, AUDIO, VIDEO, MULTIMEDIA, OFFICE, TEXT, EXECUTABLE, ARCHIVE, 3D',
  `img_major_mime`      string  COMMENT 'Possibilities are: unknown, application, audio, chemical, image, message, model, multipart, text, video',
  `img_minor_mime`      string  COMMENT 'E.g. jpeg, gif, png, svg+xml, tiff, ogg, pdf vnd.djvu, webm, wav, mpeg, x-flac, midi, x-xcf, sla, webp, etc.',
  `img_description_id`  bigint  COMMENT 'This is a foreign key to comment_id in the comment table.',
  `img_actor`           bigint  COMMENT 'This is a foreign key to actor_id in the actor table.',
  `img_timestamp`       string  COMMENT 'Timestamp of when upload took place. Not necessarily the same timestamp as logging.log_timestamp.',
  `img_sha1`            string  COMMENT 'The SHA-1 hash of the file contents in base 36 format. Wikimedia\\base_convert() is used to convert from base 16 (hexadecimal) to base 36 and then pad the result to 31 characters.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Image_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular labs imports)',
  `wiki_db` string COMMENT 'The wiki_db project')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/image'
;
