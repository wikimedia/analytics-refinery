-- Parameters:
--     destination_directory             -- HDFS path to write output files
--     source_table_per_domain           -- Fully qualified table name to compute from (per domain).
--     source_table_per_project_family   -- Fully qualified table name to compute from (per family).
--     separator                         -- Separator for values
--     year                              -- year of partition to compute from.
--     month                             -- month of partition to compute from.
--     day                               -- day of partition to compute from.
--
-- Usage:
--     hive -f unique_devices.hql                                                            \
--         -d destination_directory=/wmf/tmp/analytics/unique_devices                                      \
--         -d source_table_per_domain=wmf.unique_devices_per_domain_daily                    \
--         -d source_table_per_project_family=wmf.unique_devices_per_project_family_daily    \
--         -d separator=\t                                                                   \
--         -d year=2016                                                                      \
--         -d month=1                                                                        \
--         -d day=1                                                                          \
--


SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


WITH unique_devices_per_domain AS (
    SELECT
        CONCAT(
            regexp_extract(domain, '^((?!www)([a-z0-9-_]+)\\.)(m\\.)?\\w+\\.org$'),
            regexp_extract(domain, '([a-z0-9-_]+)\\.org$')
            ) AS project,
        CASE WHEN domain RLIKE '(^(m)\\.)|\\.m\\.'
            THEN 'mobile-site'
            ELSE 'desktop-site'
            END AS access_site,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0")) AS dt,
        SUM(uniques_estimate) AS devices,
        SUM(uniques_offset) AS offset,
        SUM(uniques_underestimate) AS underestimate
    FROM
        ${source_table_per_domain}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
    GROUP BY
        CONCAT(
            regexp_extract(domain, '^((?!www)([a-z0-9-_]+)\\.)(m\\.)?\\w+\\.org$'),
            regexp_extract(domain, '([a-z0-9-_]+)\\.org$')
            ),
        CASE WHEN domain RLIKE '(^(m)\\.)|\\.m\\.'
            THEN 'mobile-site'
            ELSE 'desktop-site'
            END,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"))
    HAVING SUM(uniques_estimate) > 1000
), unique_devices_per_domain_all_sites AS (
    SELECT
        project,
        'all-sites' AS access_site,
        dt,
        SUM(devices) AS devices,
        SUM(offset) AS offset,
        SUM(underestimate) AS underestimate
    FROM
        unique_devices_per_domain
    GROUP BY
        project,
        dt
), unique_devices_per_project_family AS (
    SELECT
        CONCAT('all-', project_family, '-projects') AS project,
        'all-sites' AS access_site,
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0")) AS dt,
        SUM(uniques_estimate) AS devices,
        SUM(uniques_offset) AS offset,
        SUM(uniques_underestimate) AS underestimate
    FROM
        ${source_table_per_project_family}
    WHERE
        year = ${year}
        AND month = ${month}
        AND day = ${day}
        AND !array_contains(array('mediawiki', 'wikidata', 'wikimediafoundation', 'wikimedia'), project_family)
    GROUP BY
        CONCAT('all-', project_family, '-projects'),
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0"))
    HAVING SUM(uniques_estimate) > 1000
), unique_devices AS (
    SELECT * FROM unique_devices_per_domain
    UNION ALL
    SELECT * FROM unique_devices_per_domain_all_sites
    UNION ALL
    SELECT * FROM unique_devices_per_project_family
)

INSERT OVERWRITE DIRECTORY "${destination_directory}"
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT
        CONCAT_WS('${separator}',
            project,
            access_site,
            dt,
            CAST(devices AS STRING),
            CAST(offset AS STRING),
            CAST(underestimate AS STRING)
        ) as line
    FROM
        unique_devices
;
