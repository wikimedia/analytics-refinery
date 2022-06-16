-- Aggregate editors_daily data into edits by country and other dimensions
-- Note: This dataset does NOT contain bots actions and only considers edit actions.
--
-- Parameters:
--     source_table         -- Read raw data from here
--     destination_table    -- Insert results here
--     month                -- YYYY-MM to compute statistics for
--     coalesce_partitions  -- Number of partitions to write
--
-- Usage:
--     hive -f geoeditors_edits_monthly.hql        \
--          -d source_table=wmf.editors_daily                  \
--          -d destination_table=wmf.geoeditors_edits_monthly  \
--          -d month=2022-02
--          -d coalesce_partitions=1
--

INSERT OVERWRITE TABLE ${destination_table}
       PARTITION (month='${month}')
     SELECT /*+ COALESCE(${coalesce_partitions}) */
            wiki_db,
            country_code,
            user_is_anonymous as edits_are_anonymous,
            sum(edit_count) as edit_count,
            sum(namespace_zero_edit_count) as namespace_zero_edit_count
       from ${source_table}
      where month = '${month}'
          -- Filter out bot actions and non-edit actions
        and size(user_is_bot_by) = 0
        and action_type IN (0, 1)
      group by wiki_db,
            country_code,
            user_is_anonymous
;