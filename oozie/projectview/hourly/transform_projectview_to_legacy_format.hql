-- Parameters:
--     source_table      -- Fully qualified table name to compute the
--                          transformation from.
--     destination_directory -- Directory where to write transformation
--                              results
--     year              -- year of partition to compute statistics for.
--     month             -- month of partition to compute statistics for.
--     day               -- day of partition to compute statistics for.
--     hour              -- hour of partition to compute statistics for.
--
-- Usage:
--     hive -f transform_projectview_to_legacy_format.hql         \
--         -d source_table=wmf.projectview_hourly                 \
--         -d destination_directory=/wmf/tmp/analytics/example                  \
--         -d year=2015                                           \
--         -d month=6                                             \
--         -d day=1                                               \
--         -d hour=1
--

SET hive.exec.compress.output        =false;
SET mapred.reduce.tasks              = 1;


INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    -- Set 0 as volume column since we don't use it.
    SELECT
        CONCAT_WS(" ", qualifier, "-", cast(view_count AS string), "0") line
    FROM (
        SELECT
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
            ) qualifier,
            SUM(view_count) AS view_count
        FROM ${source_table}
        WHERE year=${year}
            AND month=${month}
            AND day=${day}
            AND hour=${hour}
            AND agent_type = 'user'
        GROUP BY
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
            )
    ) projectview_transformed
    ORDER BY line
    LIMIT 100000
;
