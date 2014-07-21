USE ${database};
ALTER TABLE ${table}
  ADD IF NOT EXISTS
  PARTITION (${partition_spec})
  LOCATION '${location}'
;
