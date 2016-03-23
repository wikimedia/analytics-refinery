SET hive.exec.compress.output=true;
SET whitelisted_mediawiki_projects = 'commons', 'meta', 'incubator', 'species', 'strategy', 'outreach', 'usability', 'quality';
--^ To work around HIVE-3296, we have SETs before any comments

-- Extracts pagecounts from webrequests into a separate table
--
-- Usage:
--     hive -f insert_hourly_pagecounts.hql \
--         -d source_table=wmf_raw.webrequest \
--         -d destination_table=wmf.pagecounts_all_sites \
--         -d year=2014 \
--         -d month=9 \
--         -d day=15 \
--         -d hour=20
--



INSERT OVERWRITE TABLE ${destination_table}
    PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
    SELECT
        CONCAT(language_and_site, project_suffix) qualifier,
        page_title,
        COUNT(*) count_views,
        SUM(response_size) total_response_size
    FROM (
        SELECT
            regexp_extract(uri_host, '^([A-Za-z0-9-]+(\\.(zero|m))?)\\.[a-z]*\\.org$') language_and_site,
            CASE regexp_extract(uri_host, '\\.(wik(ipedia|ibooks|tionary|imediafoundation|imedia|inews|iquote|isource|iversity|ivoyage|idata)|mediawiki)\\.org$')
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
                ELSE NULL END project_suffix,
            SPLIT(TRANSLATE(SUBSTR(uri_path, 7), ' ', '_'), '#')[0] page_title,
            response_size
        FROM ${source_table}
        WHERE
            webrequest_source IN ('text')
            AND year=${year}
            AND month=${month}
            AND day=${day}
            AND hour=${hour}
            AND SUBSTR(uri_path, 1, 6) = '/wiki/'
            AND (
                    (
                        SUBSTR(ip, 1, 8) NOT IN (
                            '10.20.0.',
                            '10.64.0.'
                        ) AND SUBSTR(ip, 1, 9) NOT IN (
                            '10.128.0.',
                            '10.64.32.'
                        ) AND SUBSTR(ip, 1, 11) NOT IN (
                            '208.80.152.',
                            '208.80.153.',
                            '208.80.154.',
                            '208.80.155.',
                            '91.198.174.'
                        )
                    )
                )
            AND SUBSTR(uri_path, 1, 31) != '/wiki/Special:CentralAutoLogin/'
            AND http_status NOT IN ( '301', '302', '303' )
    ) prepared
    WHERE
        language_and_site != ''
        AND project_suffix IS NOT NULL
        AND (
            project_suffix != '.m'
            OR SPLIT(language_and_site, '\\.')[0] IN (
                ${hiveconf:whitelisted_mediawiki_projects}
            )
        )
        AND page_title != 'undefined'
        AND page_title != 'Undefined'
    GROUP BY language_and_site, project_suffix, page_title
    ORDER BY qualifier, page_title
    LIMIT 100000000;

-- In order to keep the data downward compatible with the real
-- webstatscollector, the following query inserts '.mw' counts. Those counts are
-- typically misused by people, but still, being downward compatible trumps.
--
-- (The query repeatedly does the same split on qualifier. We tried putting it
-- in a subquery, but that only slows the overall query down by ~5%, and makes
-- the query harder to read due to the subquery. Hence, we kept the single
-- query)
INSERT INTO TABLE ${destination_table}
    PARTITION(year=${year}, month=${month}, day=${day}, hour=${hour})
    SELECT
        CONCAT(SPLIT(qualifier, '\\.')[0], '.mw') qualifier,
        SPLIT(qualifier, '\\.')[0] page_title,
        SUM(count_views) count_views,
        SUM(total_response_size) total_response_size
    FROM ${destination_table}
    WHERE
        year=${year}
        AND month=${month}
        AND day=${day}
        AND hour=${hour}
        AND SPLIT(qualifier, '\\.')[1] = 'm'
        AND (
            SPLIT(qualifier, '\\.')[2] IS NOT NULL
            OR SPLIT(qualifier, '\\.')[0] NOT IN (
                ${hiveconf:whitelisted_mediawiki_projects}
            )
        )
    GROUP BY SPLIT(qualifier, '\\.')[0]
    ORDER BY qualifier, page_title
    LIMIT 100000;
