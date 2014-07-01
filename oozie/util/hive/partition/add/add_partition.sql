ADD JAR ${serde_jar};
USE ${database};
ALTER TABLE ${table}
  ADD IF NOT EXISTS
  PARTITION (${partition_spec})
  LOCATION '${location}'
;
