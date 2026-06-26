-- Creates the raw table for the Google Search Console
-- "searchdata_url_impression" bulk data export (data aggregated by URL).
--
-- Usage
--     hive -f create_google_search_console_url_impression_table.hql \
--         --database wmf_raw
--

CREATE EXTERNAL TABLE IF NOT EXISTS `google_search_console_url_impression` (
    `data_date`                         date    COMMENT 'Day on which the data in this row was generated (Pacific Time). Same value as the date partition.',
    `site_url`                          string  COMMENT 'URL of the property. Domain properties are sc-domain:property-name; URL-prefix properties are the full URL.',
    `url`                               string  COMMENT 'Fully-qualified URL where the user eventually landed.',
    `query`                             string  COMMENT 'The user query. Empty string when is_anonymized_query is true.',
    `is_anonymized_query`               boolean COMMENT 'True for rare (anonymized) queries; query is then an empty string.',
    `is_anonymized_discover`            boolean COMMENT 'True when the row is below the Discover anonymization threshold; some dimensions may be empty.',
    `country`                           string  COMMENT 'Country of the query, ISO-3166-1-Alpha-3 code.',
    `search_type`                       string  COMMENT 'Search surface: web, image, video, news, discover or googleNews.',
    `device`                            string  COMMENT 'Device used for the query (DESKTOP, MOBILE, TABLET).',
    `is_amp_top_stories`                boolean COMMENT 'Appeared as an AMP article in the Top Stories carousel.',
    `is_amp_blue_link`                  boolean COMMENT 'Appeared as an AMP standard ("blue link") result.',
    `is_job_listing`                    boolean,
    `is_job_details`                    boolean,
    `is_tpf_qa`                         boolean COMMENT 'Appeared as a Q&A rich result.',
    `is_tpf_faq`                        boolean COMMENT 'Appeared as an FAQ rich result.',
    `is_tpf_howto`                      boolean COMMENT 'Appeared as a How-to rich result.',
    `is_weblite`                        boolean COMMENT 'Served through Web Light (transcoded page).',
    `is_action`                         boolean,
    `is_events_listing`                 boolean,
    `is_events_details`                 boolean,
    `is_forums`                         boolean,
    `is_search_appearance_android_app`  boolean,
    `is_amp_story`                      boolean COMMENT 'Appeared as an AMP story (Web Story).',
    `is_amp_image_result`               boolean,
    `is_video`                          boolean,
    `is_organic_shopping`               boolean,
    `is_review_snippet`                 boolean,
    `is_special_announcement`           boolean,
    `is_recipe_feature`                 boolean COMMENT 'Appeared as a recipe feature (e.g. recipe gallery).',
    `is_recipe_rich_snippet`            boolean,
    `is_subscribed_content`             boolean,
    `is_page_experience`                boolean COMMENT 'Eligible for the page experience signal.',
    `is_practice_problems`              boolean,
    `is_math_solvers`                   boolean,
    `is_translated_result`              boolean,
    `is_edu_q_and_a`                    boolean,
    `is_product_snippets`               boolean,
    `is_merchant_listings`              boolean,
    `is_learning_videos`                boolean,
    `impressions`                       bigint,
    `clicks`                            bigint,
    `sum_position`                      bigint  COMMENT 'Sum of the zero-based topmost positions; average position = sum_position / impressions + 1.'
)
COMMENT
    'Google Search Console bulk data export, aggregated by URL.'
PARTITIONED BY (
    `date`      string COMMENT 'Day of the export partition (YYYY-MM-DD), matching data_date.',
    `domain`    string COMMENT 'Wiki family the property belongs to: wikipedia or wikimedia.'
)
STORED AS PARQUETFILE
LOCATION '/wmf/data/raw/google_search_console/searchdata_url_impression'
TBLPROPERTIES ('parquet.compression'='GZIP')
;
