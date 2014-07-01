ADD JAR ${serde_jar};
USE ${database};
ALTER TABLE ${table}
  DROP IF EXISTS
  PARTITION (${partition_spec})
;