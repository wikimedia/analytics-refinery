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
--     hive -f monthly_surviving_new_editors.hql        \
--         -d source_table=wmf.mediawiki_history        \
--         -d destination_table=wmf.mediawiki_metrics   \
--         -d start_timestamp=2001-01-01 00:00:00       \
--         -d end_timestamp=2017-01-01 00:00:00         \
--         -d wiki_db=all                               \
--         -d snapshot=2017-03

insert into table ${destination_table} partition (snapshot='${snapshot}')
select
    concat(surviving_editors.month, '-01') as dt,
    'monthly_surviving_new_editors' as metric,
    surviving_editors.wiki_db,
    count(*) as value

from (
    select
        wiki_db,
        substring(event_user_creation_timestamp, 0, 7) as month,
        event_user_id
    from ${source_table}
    where event_entity = 'revision'
        and event_type = 'create'
        and event_user_id is not null
        and event_user_is_created_by_self
        and unix_timestamp(event_timestamp) -
            unix_timestamp(event_user_creation_timestamp)
            <= 86400
        and ('${wiki_db}' = 'all' or wiki_db = '${wiki_db}')
        and event_user_creation_timestamp >= '${start_timestamp}'
        and event_user_creation_timestamp <  '${end_timestamp}'
        and snapshot = '${snapshot}'

    group by
        wiki_db,
        substring(event_user_creation_timestamp, 0, 7),
        event_user_id

    ) new_editors

inner join (
    select
        wiki_db,
        substring(event_user_creation_timestamp, 0, 7) as month,
        event_user_id
    from ${source_table}
    where event_entity = 'revision'
        and event_type = 'create'
        and unix_timestamp(event_timestamp) -
            unix_timestamp(event_user_creation_timestamp)
            between 2592000 and 5184000
        and ('${wiki_db}' = 'all' or wiki_db = '${wiki_db}')
        and event_user_creation_timestamp >= '${start_timestamp}'
        and event_user_creation_timestamp <  '${end_timestamp}'
        and snapshot = '${snapshot}'

    group by
        wiki_db,
        substring(event_user_creation_timestamp, 0, 7),
        event_user_id

    ) surviving_editors

    on surviving_editors.event_user_id = new_editors.event_user_id
        and surviving_editors.month = new_editors.month
        and surviving_editors.wiki_db = new_editors.wiki_db

group by
    surviving_editors.wiki_db,
    surviving_editors.month

;
