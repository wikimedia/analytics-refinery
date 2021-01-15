-- Parameters:
--     destination_directory -- HDFS path to write output files
--     source_table_1        -- Fully qualified projectcounts_raw table
--     source_table_2        -- Fully qualified projectcounts_all_sites table
--     source_table_3        -- Fully qualified domain_abbrev_map table
--     separator             -- Separator for values
--
-- Usage:
--     hive -f pagecounts_per_project.hql \
--         -d destination_directory=/wmf/tmp/analytics/pagecounts_per_project \
--         -d source_table_1=wmf.projectcounts_raw \
--         -d source_table_2=wmf.projectcounts_all_sites \
--         -d source_table_3=wmf.domain_abbrev_map \
--         -d separator=\t
--


SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.GzipCodec;

WITH
    -- This CTE combines projectcounts_raw and projectcounts_all_sites.
    -- projectcounts_raw has data since 2007 but not mobile counts.
    -- projectcounts_all_sites has mobile counts, but only starts on Nov 2014.
    -- see https://phabricator.wikimedia.org/T162157 meta projectcounts had major issues with quality
    combined_projectcounts AS (
        SELECT *
        FROM ${source_table_1}
        WHERE
            year <= 2013 OR
            (year = 2014 AND month <= 9) and domain_abbrev not like '%meta%'
        UNION ALL
        SELECT *
        FROM ${source_table_2}
        WHERE
            (year = 2014 AND month >= 10) OR
            year >= 2015 and domain_abbrev not like '%meta%'
    ),
    formatted_projectcounts AS (
        SELECT
            REGEXP_REPLACE(hostname, 'www\.|\.org', '') AS hostname,
            -- The formatting of the access_site final value needs to be done here in
            -- this previous step, otherwise the subsequent grouping sets won't work
            -- properly. Note that because of the inner join, the only possible values
            -- are 'desktop' or 'mobile'.
            IF(access_site = 'desktop', 'desktop-site', 'mobile-site') AS access_site,
            year,
            month,
            day,
            hour,
            view_count
        FROM combined_projectcounts as pc
        INNER JOIN ${source_table_3} as ab
        ON pc.domain_abbrev = ab.domain_abbrev
    )

INSERT OVERWRITE DIRECTORY '${destination_directory}'
    -- Since "ROW FORMAT DELIMITED DELIMITED FIELDS TERMINATED BY ' '" only
    -- works for exports to local directories (see HIVE-5672), we have to
    -- prepare the lines by hand through concatenation :-(
    SELECT
        CONCAT_WS('${separator}',
            COALESCE(hostname, 'all-projects'),
            COALESCE(access_site, 'all-sites'),
            IF(day IS NULL, 'monthly', IF(hour IS NULL, 'daily', 'hourly')),
            CONCAT(
                LPAD(year, 4, '0'),
                LPAD(month, 2, '0'),
                LPAD(COALESCE(day, 1), 2, '0'),
                LPAD(COALESCE(hour, 0), 2, '0')
            ),
            CAST(SUM(view_count) AS STRING)
        )
    FROM formatted_projectcounts
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
