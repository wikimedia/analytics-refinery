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
--     hive -f monthly_new_registered_users.hql         \
--         -d source_table=wmf.mediawiki_history        \
--         -d destination_table=wmf.mediawiki_metrics   \
--         -d start_timestamp=2001-01-01 00:00:00       \
--         -d end_timestamp=2017-01-01 00:00:00         \
--         -d wiki_db=all                               \
--         -d snapshot=2017-03

insert into table ${destination_table} partition (snapshot='${snapshot}')
select
    concat(substring(event_user_creation_timestamp, 0, 7), '-01') as dt,
    'monthly_new_registered_users' as metric,
    wiki_db,
    count(*) as value

from ${source_table}
where event_entity = 'user'
    and event_type = 'create'
    and user_is_created_by_self
    and ('${wiki_db}' = 'all' or wiki_db = '${wiki_db}')
    and event_timestamp >= '${start_timestamp}'
    and event_timestamp <  '${end_timestamp}'
    and snapshot = '${snapshot}'

group by
    wiki_db,
    substring(event_user_creation_timestamp, 0, 7)

;
