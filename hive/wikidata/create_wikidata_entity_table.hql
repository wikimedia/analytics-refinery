-- Creates table statement for wikidata_entity table.
--
-- Parameters:
--     <none>
--
-- Usage
--     hive -f create_wikidata_entity_table.hql \
--         --database wmf
--

CREATE EXTERNAL TABLE `wikidata_entity` (
    `id` string                            COMMENT 'The id of the entity, P31 or Q32753077 for instance',
    `typ` string                           COMMENT 'The type of the entity, property or item for instance',
    `labels` map<string, string>           COMMENT 'The language/label map of the entity',
    `descriptions` map<string, string>     COMMENT 'The language/description map of the entity',
    `aliases` map<string, array<string>>   COMMENT 'The language/List-of-aliases map of the entity',
    `claims` array<struct<
        `id`: string,
        `mainSnak`: struct<
            `typ`: string,
            `property`: string,
            `dataType`: string,
            `dataValue`: struct<
                `typ`: string,
                `value`: string>,
            `hash`: string>,
        `typ`: string,
        `rank`: string,
        `qualifiers`: array<struct<
            `typ`: string,
            `property`: string,
            `dataType`: string,
            `dataValue`: struct<
                `typ`: string,
                `value`: string>,
            `hash`: string>>,
        `references`: array<struct<
            `snaks`: array<struct<
                `typ`: string,
                `property`: string,
                `dataType`: string,
                `dataValue`: struct<
                    `typ`: string,
                    `value`: string>,
                `hash`: string>>,
            `order`: array<string>,
            `hash`: string>>>>             COMMENT 'The claim array of the entity',
    `sitelinks` array<struct<
        `site`: string,
        `title`: string,
        `badges`: array<string>,
        `url`: string>>                    COMMENT 'The siteLinks array of the entity'
)
COMMENT
    'See most up to date documentation at https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/Wikidata_entity'
PARTITIONED BY (
    `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM-DD for regular weekly imports)'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/wikidata/entity'
;

