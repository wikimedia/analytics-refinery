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
 select * from ${source_table}
  where year=${year}
    and month=${month}
    and day=${day};
