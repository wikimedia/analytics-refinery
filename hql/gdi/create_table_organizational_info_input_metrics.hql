CREATE EXTERNAL TABLE IF NOT EXISTS gdi.organizational_info_input_metrics (
  `unique_id`                                    string             COMMENT 'Used to distinguish each entry even when the other values change. It does not need to follow a specific pattern but it does need to be unique.',
  `affiliate_code`                               string             COMMENT 'Affiliate code for the organization',
  `group_name`                                   string             COMMENT 'The name used by the affiliate (User Group, Chapter etc). This should in most cases be a recognized affiliate listed on https://meta.wikimedia.org/wiki/Wikimedia_movement_affiliates',
  `org_type`                                     string             COMMENT 'Type of organization for example; User Group, Thematic Organization, Chapter etc.',
  `region`                                       string             COMMENT 'The geographical location of the affiliate based on the 7 continents that exist.',
  `group_country`                                string             COMMENT 'The country that is affiliate was recognized in. Basically, where the application process happened and where the group contacts are.',
  `legal_entity`                                 string             COMMENT 'Yes or No if the organization is a legal entity',
  `mission_changed`                              string             COMMENT 'Yes or No if the mission has changed over the years',
  `group_page_url`                               string             COMMENT 'URL of the organization page on Wiki and this is usually on Meta-Wiki',
  `member_count`                                 int                COMMENT 'Number of members in the organization',
  `non_editors_count`                            int                COMMENT 'Number of non-editors in the organization',
  `affiliate_size`                               int                COMMENT 'Size of the organization on a scale of 1-5 where 1=<10, 2=10-29, 3=30-50, 4=51-100, 5=>100',
  `facebook_url`                                 string             COMMENT 'URL of the Facebook page',
  `twitter_url`                                  string             COMMENT 'URL of the Twitter page',
  `other_url`                                    string             COMMENT 'URL to any other presence of the organization which is different from Facebook or Twitter (maybe mailing list, blog or news page)',
  `dm_structure`                                 array<string>      COMMENT 'List of the decion Making Structure of the organization',
  `group_contact_1`                              string             COMMENT 'Primary contact person for the organization',
  `group_contact_2`                              string             COMMENT 'Secondary contact person for the organization',
  `board_contacts`                               array<string>      COMMENT 'List of board contacts for the organization',
  `agreement_date`                               string             COMMENT 'Resolution date from the Affiliations Committee to the group in DD/MM/YYYY',
  `fiscal_year_start`                            string             COMMENT 'Fiscal year start date in DD/MM',
  `fiscal_year_end`                              string             COMMENT 'Fiscal year end date in DD/MM',
  `me_bypass_ooc_autochecks`                     string             COMMENT 'Yes or No if M&E staff should by pass special actions on the affiliate or not',
  `out_of_compliance_level`                      int                COMMENT 'Shows the level of OOC (out of compliance) that the affiliate is at based on system checks and M&E actions',
  `reporting_due_date`                           date               COMMENT 'The date that the affiliate should have reported on their activities. This is useful when computing the out of compliance status.',
  `uptodate_reporting`                           string             COMMENT 'Concludes if an affiliate is in compliance or not with a Tick or a Cross string respectively. In addition, special cases for new affiliates (such as User Groups), M&E staff shall input Tick-N or Cross-N for field.',
  `notes_on_reporting`                           string             COMMENT 'Additional notes from the Monitoring and Evaluation Staff about the compliance status of the affiliate',
  `recognition_status`                           string             COMMENT 'The status of the organization based on the recognition process. This is a string that can be either "recognized" or "derecognized"',
  `derecognition_date`                           string             COMMENT 'The date that the organization was derecognized. This is a string in DD/MM/YYYY format',
  `derecognition_notes`                          string             COMMENT 'Notes on the derecognition of the organization',
  `dos_stamp`                                    string             COMMENT 'Date of submission stamp and this is the date that this entry/report was submitted. This is a string in YYYY/MM/DD format',
  `affiliate_tenure`                             double             COMMENT 'The number of years that the affiliate has been recognized'
)
COMMENT
  'Keeps track of Organizational Information submitted by Wikimedia Affiliates'
PARTITIONED BY (
  `year` int COMMENT 'The year in YYYY format'
)
STORED AS PARQUET
LOCATION
  'hdfs://analytics-hadoop/wmf/data/gdi/organizational_info_input_metrics'
;
