-- Archive one partition
--
-- Usage:
--     hive -f archive.hql                              \
--         -d source_table=milimetric.referrer_daily    \
--         -d destination_directory=/tmp/archive_test   \
--         -d year=2021                                 \
--         -d month=3                                   \
--         -d day=3                                     \
--

 insert overwrite directory '${destination_directory}'
    ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'

-- hack: this space -> '[ ]country' has to be there for the header to sort first
 select ' country' as country,
        'lang',
        'browser_family',
        'os_family',
        'search_engine',
        'num_referrals',
        'day'

  union all

 select country,
        lang,
        browser_family,
        os_family,
        search_engine,
        cast(num_referrals as string),
        CONCAT(LPAD(year, 4, "0"), LPAD(month, 2, "0"), LPAD(day, 2, "0")) AS day

   from ${source_table}
  where year=${year}
    and month=${month}
    and day=${day}
  order by country
  limit 1000000
;
