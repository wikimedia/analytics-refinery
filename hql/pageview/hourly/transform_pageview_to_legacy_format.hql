-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          transformation from.
--     destination_directory -- Directory where to write transformation
--                              results
--     year              -- year of partition to compute statistics for.
--     month             -- month of partition to compute statistics for.
--     day               -- day of partition to compute statistics for.
--     hour              -- hour of partition to compute statistics for.
--     coalesce_partitions   -- the number of final partitions.
--
-- Usage:
--     spark3-sql --master yarn -f transform_pageview_to_legacy_format.hql  \
--         -d source_table=wmf.pageview_hourly                              \
--         -d destination_directory=/tmp/wmf/analytics/example              \
--         -d year=2023                                                     \
--         -d month=2                                                       \
--         -d day=7                                                         \
--         -d hour=1                                                        \
--         -d coalesce_partitions=1
--

SET spark.hadoop.hive.exec.compress.output = true;

WITH formatted as (
 SELECT /*+ COALESCE(${coalesce_partitions}) */
    CONCAT(
        -- Core identifier and mobile
        CASE regexp_extract(project, '^([A-Za-z0-9-]+)\\.[a-z]*$')
            WHEN '' THEN (
                --mobile if any, www otherwise
                CASE
                    WHEN COALESCE(access_method, '') IN ('mobile web', 'mobile app') THEN 'm'
                    ELSE 'www'
                END
            )
            ELSE (
                -- Project ident plus mobile suffix if any
                CASE
                    WHEN COALESCE(access_method, '') IN ('mobile web', 'mobile app')
                        THEN CONCAT(regexp_extract(project, '^([A-Za-z0-9-]+)\\.[a-z]*$'), '.m')
                    ELSE regexp_extract(project, '^([A-Za-z0-9-]+)\\.[a-z]*$')
                END
            )
        END,
        -- Project suffix, made NULL if not found
        CASE regexp_extract(project, '^([A-Za-z0-9-]+\\.)?(wik(ipedia|ibooks|tionary|imediafoundation|imedia|inews|iquote|isource|iversity|ivoyage|idata)|mediawiki)$', 2)
            WHEN 'wikipedia' THEN ''
            WHEN 'wikibooks' THEN '.b'
            WHEN 'wiktionary' THEN '.d'
            WHEN 'wikimediafoundation' THEN '.f'
            WHEN 'wikimedia' THEN '.m'
            WHEN 'wikinews' THEN '.n'
            WHEN 'wikiquote' THEN '.q'
            WHEN 'wikisource' THEN '.s'
            WHEN 'wikiversity' THEN '.v'
            WHEN 'wikivoyage' THEN '.voy'
            WHEN 'mediawiki' THEN '.w'
            WHEN 'wikidata' THEN '.wd'
            ELSE NULL
        END
    ) as qualifier,
    page_title,
    view_count

   FROM ${source_table}
  WHERE year=${year}
    AND month=${month}
    AND day=${day}
    AND hour=${hour}
    AND agent_type = 'user'
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
USING csv
OPTIONS ('compression' 'gzip', 'sep' ' ')
 SELECT qualifier,
        page_title,
        sum(view_count),
        -- for backwards compatibility, a weird historical artifact
        -- we live with until we deprecate this dataset
        0 as size
   FROM formatted
  GROUP BY qualifier, page_title
  ORDER BY qualifier, page_title
;
