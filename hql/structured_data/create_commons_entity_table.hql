-- Creates table statement for commons_entity table.
--
-- Parameters:
--     <none>
--
-- Usage
--     spark3-sql -f create_commons_entity_table.hql \
--         --database structured_data
--

CREATE EXTERNAL TABLE `commons_entity` (
    `id` string                            COMMENT 'The id of the entity, M123 for instance',
    `typ` string                           COMMENT 'The type of the entity',
    `labels` map<string, string>           COMMENT 'The language/label map of the entity',
    `descriptions` map<string, string>     COMMENT 'The language/description map of the entity',
    `statements` array<struct<
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
        `qualifiersOrder`: array<string>,
        `references`: array<struct<
            `snaks`: array<struct<
                `typ`: string,
                `property`: string,
                `dataType`: string,
                `dataValue`: struct<
                    `typ`: string,
                    `value`: string>,
                `hash`: string>>,
            `snaksOrder`: array<string>,
            `hash`: string>>>>             COMMENT 'The claim array of the entity',
    `lastRevId` bigint                     COMMENT 'The latest revision id of the entity'
)
COMMENT
    'See most up to date documentation at https://wikitech.wikimedia.org/wiki/Analytics/Data_Lake/Edits/Structured_data/Commons_entity'
PARTITIONED BY (
    `snapshot` string COMMENT 'Versioning information to keep multiple datasets (YYYY-MM-DD for regular weekly imports)'
)
STORED AS PARQUET
LOCATION '/wmf/data/wmf/structured_data/commons/entity'
;

