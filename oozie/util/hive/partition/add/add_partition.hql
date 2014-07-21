-- Since ALTER TABLE does not handle fully qualified table names, we
-- have to require database an table as separate parameters.
USE ${database};
ALTER TABLE ${table}
  ADD IF NOT EXISTS
  PARTITION (${partition_spec})
  LOCATION '${location}'
;
