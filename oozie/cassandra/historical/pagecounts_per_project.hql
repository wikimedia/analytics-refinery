-- Parameters:
--     destination_directory -- HDFS path to write output files
--     source_table_1        -- Fully qualified projectcounts table
--     source_table_2        -- Fully qualified abbreviation map table
--     separator             -- Separator for values
--
-- Usage:
--     hive -f pagecounts_per_project.hql \
--         -d destination_directory=/tmp/pagecounts_per_project \
--         -d source_table_1=wmf.projectcounts_raw \
--         -d source_table_2=wmf.domain_abbrev_map \
--         -d separator=\t
--


SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;


INSERT OVERWRITE DIRECTORY '${destination_directory}'
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT
        CONCAT_WS('${separator}',
            COALESCE(REGEXP_REPLACE(hostname, '\.org', ''), 'all-projects'),
            CASE
                WHEN access_site = 'desktop' THEN 'desktop-site'
                WHEN access_site IN ('mobile', 'zero') THEN 'mobile-site'
                ELSE 'all-sites'
            END,
            IF(day IS NULL, 'monthly', IF(hour IS NULL, 'daily', 'hourly')),
            CONCAT(
                LPAD(year, 4, '0'),
                LPAD(month, 2, '0'),
                LPAD(COALESCE(day, 1), 2, '0'),
                LPAD(COALESCE(hour, 0), 2, '0')
            ),
            CAST(SUM(view_count) AS STRING)
        )
    FROM
        ${source_table_1} AS pc
    INNER JOIN
        ${source_table_2} AS ab
    ON
        pc.domain_abbrev = ab.domain_abbrev
    WHERE
        year >= 2007 AND year <= 2016
    GROUP BY
        hostname,
        access_site,
        year,
        month,
        day,
        hour
    GROUPING SETS (
        (hostname, access_site, year, month, day, hour),
        (hostname, access_site, year, month, day),
        (hostname, access_site, year, month),
        (hostname, year, month, day, hour),
        (hostname, year, month, day),
        (hostname, year, month),
        (access_site, year, month, day, hour),
        (access_site, year, month, day),
        (access_site, year, month),
        (year, month, day, hour),
        (year, month, day),
        (year, month)
    );
