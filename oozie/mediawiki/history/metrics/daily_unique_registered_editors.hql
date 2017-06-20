-- Parameters:
--  source_table    -- Fully qualified table name with mediawiki history.
--  start_timestamp -- YYYYMMDDHHmmss formatted start (inclusive)
--  end_timestamp   -- YYYYMMDDHHmmss formatted end (exclusive)
--  wiki_db         -- Optionally filter by source wiki database.
--                      Pass 'all' to compute for all wikis
--  snapshot        -- The snapshot to compute the metric for
--                      YYYY-MM for regular labs imports
--
-- Usage:
--     hive -f daily_unique_registered_editors.hql      \
--         -d source_table=wmf.mediawiki_history        \
--         -d destination_table=wmf.mediawiki_metrics   \
--         -d start_timestamp=2001-01-01 00:00:00       \
--         -d end_timestamp=2017-01-01 00:00:00         \
--         -d wiki_db=all                               \
--         -d snapshot=2017-03

set hive.mapred.mode=nonstrict;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.error.on.empty.partition=false;
-- there are no more than 900 wikis, no matter how we import, usually less
set hive.exec.max.dynamic.partitions=2000;
-- and we only use one node
set hive.exec.max.dynamic.partitions.pernode=2000;

-- dynamic partitions must be specified here
 insert overwrite table ${destination_table} partition (snapshot='${snapshot}', metric, wiki_db)
-- dynamic partitions must be selected in order and at the end
 select substring(event_timestamp, 0, 10) as dt,
        count(distinct event_user_id) as value,
        'daily_unique_registered_editors' as metric,
        wiki_db

   from ${source_table}
  where event_entity = 'revision'
    and event_type = 'create'
    and not array_contains(event_user_groups, 'bot')
    and not event_user_is_anonymous
    and ('${wiki_db}' = 'all' or wiki_db = '${wiki_db}')
    and event_timestamp >= '${start_timestamp}'
    and event_timestamp <  '${end_timestamp}'
    and snapshot = '${snapshot}'

  group by wiki_db,
        substring(event_timestamp, 0, 10)
  order by wiki_db,
        dt
;
