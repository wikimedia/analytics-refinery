-- Creates table statement for raw mediawiki_filerevision table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_filerevision_table.hql \
--         --database wmf_raw
--


CREATE EXTERNAL TABLE `mediawiki_filerevision`(
  `fr_id`               bigint  COMMENT 'Primary key, autoincrement. EXAMPLE: 1476994',
  `fr_file`             bigint  COMMENT 'Foreign key to file.file_id. EXAMPLE: 941821',
  `fr_size`             bigint  COMMENT 'File size in bytes. EXAMPLE: 81399',
  `fr_width`            bigint  COMMENT 'Image width, in pixels. EXAMPLE: 270',
  `fr_height`           bigint  COMMENT 'Image height, in pixels. EXAMPLE: 369',
  -- As of 2026-05 the natural breakpoint is about 3.2K chars for fr_metadata
  -- so for the big ones we do a synthetic replacement instead to avoid
  -- starving resources in the data lake or downstream.
  `fr_metadata`         string  COMMENT 'JSON text or serialized PHP text (utf8mb4-cast). Rows where OCTET_LENGTH > 3200 will contain JSON {"too_big": 1} at sqoop time. EXAMPLE: {"data":{"ExifVersion":"0221","ColorSpace":1,"MEDIAWIKI_EXIF_VERSION":2}}',
  `fr_metadata_byte_len` bigint COMMENT 'OCTET_LENGTH(fr_metadata) original - synthetic field. Big number if fr_metadata is {"too_big": 1}. EXAMPLE: 3333',
  `fr_bits`             int     COMMENT 'Bits per pixel for the revision of the file. EXAMPLE: 8',
  `fr_description_id`   bigint  COMMENT 'Foreign key to comment.comment_id for revision. EXAMPLE: 386237286',
  `fr_actor`            bigint  COMMENT 'Foreign key to actor.actor_id. EXAMPLE: 16684',
  `fr_timestamp`        string  COMMENT 'Timestamp of when the file revision was created. EXAMPLE: 20250518000001',
  `fr_sha1`             string  COMMENT 'The SHA-1 hash of the file contents in base-36 format. EXAMPLE: 78a3ff78gfbihwlhx34ogudyfukx493',
  `fr_archive_name`     string  COMMENT 'The archive filename when the revision was superseded. EXAMPLE: Foo.jpg',
  `fr_deleted`          int     COMMENT 'Whether revision is deleted. EXAMPLE: 1'
)
COMMENT
  'See https://www.mediawiki.org/wiki/Manual:Filerevision_table'
PARTITIONED BY (
  `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM for regular cloud imports).',
  `wiki_db` string COMMENT 'The wiki_db project.')
ROW FORMAT SERDE
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/filerevision'
;
