-- Creates table statement for raw mediawiki_archive table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_mediawiki_archive_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE `mediawiki_archive`(
  `ar_id`               bigint      COMMENT 'Primary key along with wiki.',
  `ar_namespace`        int         COMMENT 'Basic page information: contains the namespace of the deleted revision. These contain the value in page_namespace.',
  `ar_title`            string      COMMENT 'Basic page information: contains the page title of the deleted page, which is the same as page_title.',
  `ar_text`             string      COMMENT 'Before MediaWiki 1.5, old archived pages saved their text here. Old archived pages have ar_text_id set to NULL\; they do not point to an entry in the text table. Instead, this field is the place where the text resides. Restoring content from an old archived page restores the text from this field.  MediaWiki version: ≥ 1.5 Still used! In newly deleted pages (MediaWiki 1.5 and later), the revision text remains in the text table. Such newly deleted pages will not store text in the archive table, but will rather reference the separately existing text rows. However, for text from pages, which have been archived in MediaWiki 1.4 and before, the ar_text field will still be used!  This field remains for backward compatibility.',
  `ar_comment`          string      COMMENT 'Basic revision information: contains the edit summary of the deleted revision, analogous to rev_comment.',
  `ar_user`             bigint      COMMENT 'Basic revision information: contains the user ID of the user who made the deleted revision\; it is the same as user_id and rev_user. The value for this field is 0 for anonymous edits, initializations scripts, and for some mass imports.',
  `ar_user_text`        string      COMMENT 'Basic revision information: This field contains the text of the editor\'s username, or the IP address of the editor if the deleted revision was done by an unregistered user. Comparable to rev_user_text.',
  `ar_timestamp`        string      COMMENT 'This field contains the time at which the revision was originally saved. It is the equivalent of rev_timestamp.  Note Note: This is not the timestamp of article deletion\; that is saved in the deletion log entry, in the logging table\'s log_timestamp.',
  `ar_minor_edit`       boolean     COMMENT 'Basic revision information: Records whether the user marked the deleted revision as a minor edit. If the value for this field is 1, then the edit is tagged as "minor"\; it is 0 otherwise. This is equivalent to rev_minor_edit.',
  `ar_flags`            string      COMMENT 'This field is similar to old_flags in the text table. It was added in MediaWiki 1.5, but is most likely unused: For revisions archived with older versions, it is not used as it was not there at the time of their deletion\; a conversion, which would have added flags for revisions, which already had been archived when the update to MediaWiki 1.5 or newer was being done, was not applied. Texts of revisions archived with MediaWiki 1.5 or later do not use this field\; they themselves stay in the text table\; together with their flags staying in old_flags.',
  `ar_rev_id`           bigint      COMMENT 'When revisions are deleted, their unique rev_id is stored here so it can be retained after undeletion. This is necessary to retain permalinks to given revisions after accidental delete cycles or messy operations like history merges.  Note: Old entries from 1.4 will be NULL here, and a new rev_id will be created on undeletion for those revisions.',
  `ar_text_id`          bigint      COMMENT 'For revisions deleted in MediaWiki 1.5 and later, this is a key to old_id within the text table\; that is, it is the key to the stored text in the storage backend. To avoid breaking the block-compression scheme and otherwise making storage changes harder, the actual text is *not* deleted from the text table\; rather, the text is merely hidden by removal of the page and revision entries. Comparable to rev_text_id.  Note Note: Old entries deleted under MediaWiki 1.2-1.4 will have NULL values in this field, and their ar_text and ar_flags fields will be used to create a new text row upon undeletion.',
  `ar_deleted`          int         COMMENT 'This field is reserved for the RevDelete/Suppression (Oversight) system. Equivalent to rev_deleted.',
  `ar_len`              bigint      COMMENT 'This field contains the length of the deleted revision, in bytes. Analogous to rev_len.',
  `ar_page_id`          bigint      COMMENT 'Reference to page_id. Useful for sysadmin fixing of large pages merged together in the archives, or for cleanly restoring a page at its original ID number if possible. Comparable to rev_page. Will be NULL for pages deleted prior to 1.11.',
  `ar_parent_id`        bigint      COMMENT 'The revision id of the previous revision to the page. Populated from rev_parent_id. Will be null for revisions deleted prior to 1.13. First edits to newly created articles (and therefore the creation of the article) can be identified by the value of this field being 0.',
  `ar_sha1`             string      COMMENT 'The SHA-1 text content hash in base-36. Populated from rev_sha1.',
  `ar_content_model`    string      COMMENT 'Content model for the archived revision, which is NULL by default and only stored if it differs from the page\'s default, as determined by ContentHandler::getDefaultModelFor( $title ).',
  `ar_content_format`   string      COMMENT 'Content format for the archived revision, which is NULL by default and only stored if it differs from the page\'s default.',
  `ar_actor`            bigint      COMMENT 'This is a foreign key to actor_id in the actor table.',
  `ar_comment_id`       bigint      COMMENT 'This is a foreign key to comment_id in the comment table.'
)
COMMENT
  'See most up to date documentation at https://www.mediawiki.org/wiki/Manual:Archive_table'
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
  'hdfs://analytics-hadoop/wmf/data/raw/mediawiki/tables/archive'
;
