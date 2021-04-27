This directory contains allowlists for RefineSanitize jobs.

Each allowlist maps from a Hive table and fields to instructions
about what RefineSanitize should do with that table and field.

## Allowlists

### event_sanitized_analytics
Used for sanitizing 'analytics' tables from the Hive `event` database into the Hive
`event_sanitized` database.  Only tables and fields listed here will
be refined into `event_sanitized`.  The job that uses this allowlist
does not allow usage of the `keep_all` tag.

### event_sanitized_main
Used for sanitizing 'non-analytics' tables from the Hive `event` database into the Hive
`event_sanitized` database.  Only tables and fields listed here will
be refined into `event_sanitized`.  The job that uses this allowlist
does allow usage of the `keep_all` tag.


