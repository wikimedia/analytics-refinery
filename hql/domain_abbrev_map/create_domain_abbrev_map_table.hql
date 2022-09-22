--
-- Create table statement for domain abreviation map
--
-- This is a helper table that permits (by joining to it) translating
-- webstatscollector domain abbreviations into their respective full
-- domains and access sites, i.e.:
--
--     en         <->  en.wikipedia.org, desktop
--     de.m.b     <->  de.wikibooks.org, mobile
--
-- The contents from this table come from the execution of the script
-- bin/generate-domain-abbrev-map in this repository.
--
-- Usage
--     hive -f create_domain_abbrev_map_table.hql --database wmf
--

CREATE EXTERNAL TABLE IF NOT EXISTS `domain_abbrev_map`(
  `domain_abbrev`  string  COMMENT 'Webstatscollector domain abbreviation',
  `hostname`       string  COMMENT 'Full domain hostname (en.wikipedia.org)',
  `access_site`    string  COMMENT 'Accessed site (desktop|mobile)'
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION '/wmf/data/archive/domain_abbrev_map'
;
