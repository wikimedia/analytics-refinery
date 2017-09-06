# Mediawiki History Metrics using Druid

This document present the metrics we plan on compute in Druid, requested and served
through AQS.


## Classical timeseries metrics

Data points over a single measure, interval not limited, daily or monthly granularities,
with filters


### Definitions

* new articles
  * Formula: page create - page delete + page restore
  * Granularities: daily or monthly
  * Filters: project, page type


* edited articles
  * Formula: count distinct article with at least one edit
  * Granularities: daily or monthly
  * Filters: project, page type, activity level (only monthly)


* new users
  * Formula: user create (created by self, to prevent auto-create)
  * Granularities: daily or monthly
  * Filters: project, user type


* editors
  * Formula: count distinct user with an edit
  * Granularities: daily or monthly
  * Filters: project, user type, page type, activity level (only monthly)


* edits
  * Formula: count distinct edits
  * Granularities: daily or monthly
  * Filters: project, user type, page type


* added bytes
  * Formula: sum of text_bytes_diff (positive and negative values, result can be 0)
  * Granularities: daily or monthly
  * Filters: project, user type, page type


* modified bytes
  * Formula: sum of absolute value of text_bytes_diff
  * Granularities: daily or monthly
  * Filters: project, user type, page type


### Druid examples

Can be run as is from any stat100X machine.


* new articles -- Daily, whole 2016, en.wikipedia, content only
  * Time - NOT cached:  105ms
  * Time - Cached:       34ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "timeseries",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "day",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "page" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "1" }
    ]
  },
  "aggregations": [
      {
          "type" : "filtered",
          "filter" : { "type": "selector", "dimension": "event_type", "value": "create" },
          "aggregator" : { "type": "count", "name": "articles_created" }
      },
      {
          "type" : "filtered",
          "filter" : { "type": "selector", "dimension": "event_type", "value": "delete" },
          "aggregator" : { "type": "count", "name": "articles_deleted" }
      },
      {
          "type" : "filtered",
          "filter" : { "type": "selector", "dimension": "event_type", "value": "restore" },
          "aggregator" : { "type": "count", "name": "articles_restored" }
      }
    ],
    "postAggregations" : [
        {
            "type" : "arithmetic",
            "name" : "new_articles",
            "fn" : "-",
            "fields" : [
                {
                    "type"     : "arithmetic",
                    "name"     : "created_plus_restored",
                    "fn"         : "+",
                    "fields" : [
                        { "type" : "fieldAccess", "fieldName" : "articles_created" },
                        { "type" : "fieldAccess", "fieldName" : "articles_restored" }
                    ]
                },
                { "type" : "fieldAccess", "fieldName" : "articles_deleted" }
            ]
        }
    ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* edited articles -- Monthly, whole 2016, en.wikipedia, content only, page-month-activity-level more than 5 included
  * Time - NOT cached:  3531ms
  * Time - Cached:        23ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "timeseries",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "1" },
      { "type": "regex", "dimension": "page_month_activity_level", "pattern": "^([56789]|\\d{1}\\d+)$" }
    ]
  },
  "aggregations": [
      {
        "type": "cardinality",
        "name": "edited_articles",
        "fields": [ "page_id" ]
      }
    ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* new users -- Monthly, whole 2016, en.wikipedia, users only
  * Time - NOT cached:   553ms
  * Time - Cached:        23ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "timeseries",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "user" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "event_user_is_anonymous", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_group", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_name", "value": "0" }
    ]
  },
  "aggregations": [
      {
        "type": "cardinality",
        "name": "editors",
        "fields": [ "event_user_id" ]
      }
    ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* editors -- Monthly, whole 2016, en.wikipedia, bot by group only, content only, user-activity-level between 5 and 99 included
  * Time - NOT cached:   529ms
  * Time - Cached:        22ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "timeseries",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "event_user_is_anonymous", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_group", "value": "1" },
      { "type": "selector", "dimension": "event_user_is_bot_by_name", "value": "0" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "1" },
      { "type": "regex", "dimension": "event_user_month_activity_level", "pattern": "^([56789]|\\d{2})$" }
    ]
  },
  "aggregations": [
      { "type": "longSum", "name": "user_create", "fieldName": "events" }
    ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* edits -- Daily, whole 2016, en.wikipedia, user only, content only
  * Time - NOT cached:   268ms
  * Time - Cached:        28ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "timeseries",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "day",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "event_user_is_anonymous", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_group", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_name", "value": "0" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "1" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "edits", "fieldName": "events" }
  ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* added bytes -- Daily, whole 2016, en.wikipedia, anonymous only, non-content only
  * Time - NOT cached:   168ms
  * Time - Cached:        24ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "timeseries",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "day",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "event_user_is_anonymous", "value": "1" },
      { "type": "selector", "dimension": "event_user_is_bot_by_group", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_name", "value": "0" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "0" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "added_bytes", "fieldName": "text_bytes_diff" }
  ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* modified bytes -- Daily, whole 2016, en.wikipedia, anonymous only, content only
  * Time - NOT cached:   180ms
  * Time - Cached:        26ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "timeseries",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "day",
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "event_user_is_anonymous", "value": "1" },
      { "type": "selector", "dimension": "event_user_is_bot_by_group", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_name", "value": "0" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "1" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "modified_bytes", "fieldName": "text_bytes_diff_abs" }
  ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


## Classical TopN metrics

Top 100 over a single measure, interval limited to one of the chosen granularity, daily
and monthly granularities, with filters


### Definitions

* Most edited articles
  * Formula: top 100 page_title by number of edits
  * Granularities: daily or monthly
  * Filters: project, page type


* Articles with most contributors
  * Formula: top 100 page_title by number of distinct event_user_text (to mitigate anonymous)
  * Granularities: daily or monthly
  * Filters: project, page type


* Articles with largest growth
  * Formula: top 100 page_title by sum of text_bytes_diff
  * Granularities: daily or monthly
  * Filters: project, page type


* Articles most modified (bytes)
  * Formula: top 100 page_title by sum of text_bytes_diff_abs
  * Granularities: daily or monthly
  * Filters: project, page type


* Contributors with most edits
  * Formula: top 100 event_user_text by number of edits
  * Granularities: daily or monthly
  * Filters: project, user type


* Contributors having added most bytes
  * Formula: top 100 event_user_text by sum of text_bytes_diff
  * Granularities: daily or monthly
  * Filters: project, user type


* Contributors having modified most bytes
  * Formula: top 100 event_user_text by sum of text_bytes_diff_abs
  * Granularities: daily or monthly
  * Filters: project, user type


### Druid examples ###

Can be run as is from any stat100X machine.

* Most edited articles -- Monthly, 2017-07, en.wikipedia, content type
  * Time - NOT cached:   916ms
  * Time - Cached:        23ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "dimension": "page_title",
  "metric": "edits",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "1" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "edits", "fieldName": "events" }
  ],
  "intervals": [ "2017-07-01T00:00:00.000/2017-08-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* Articles with most contributors -- Monthly, 2017-06, en.wikipedia, non-content type

  **Results for this query seem incorrect ... Crap...**

  * Time - NOT cached:  10411ms
  * Time - Cached:         27ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "dimension": "page_title",
  "metric": "contributors",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "0" }
    ]
  },
  "aggregations": [
    {
      "type": "cardinality",
      "name": "contributors",
      "fields": [ "user_event_text" ]
    }
  ],
  "intervals": [ "2017-06-01T00:00:00.000/2017-07-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* Articles with largest growth -- Monthly, 2017-05, en.wikipedia, content type
  * Time - NOT cached:  1029ms
  * Time - Cached:        23ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "dimension": "page_title",
  "metric": "added_bytes",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "1" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "added_bytes", "fieldName": "text_bytes_diff" }
  ],
  "intervals": [ "2017-05-01T00:00:00.000/2017-06-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* Articles most modified (bytes) -- Monthly, 2017-04, en.wikipedia, non-content type
  * Time - NOT cached:   1153ms
  * Time - Cached:         22ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "dimension": "page_title",
  "metric": "modified_bytes",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "page_namespace_is_content", "value": "0" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "modified_bytes", "fieldName": "text_bytes_diff_abs" }
  ],
  "intervals": [ "2017-04-01T00:00:00.000/2017-05-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* Contributors with most edits -- Monthly, 2017-07, en.wikipedia, user type
  * Time - NOT cached:   415ms
  * Time - Cached:        22ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "dimension": "event_user_text",
  "metric": "edits",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "event_user_is_anonymous", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_group", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_name", "value": "0" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "edits", "fieldName": "events" }
  ],
  "intervals": [ "2017-07-01T00:00:00.000/2017-08-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* Contributors having added most bytes -- Daily, 2017-07-27, en.wikipedia, bot by group type
  * Time - NOT cached:    62ms
  * Time - Cached:        22ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "day",
  "dimension": "event_user_text",
  "metric": "added_bytes",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "event_user_is_anonymous", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_group", "value": "1" },
      { "type": "selector", "dimension": "event_user_is_bot_by_name", "value": "0" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "added_bytes", "fieldName": "text_bytes_diff" }
  ],
  "intervals": [ "2017-07-27T00:00:00.000/2017-07-28T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


* Contributors having modified most bytes -- Monthly, 2016-03, en.wikipedia, anonymous
  * Time - NOT cached:   595ms
  * Time - Cached:        42ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "day",
  "dimension": "event_user_text",
  "metric": "modified_bytes",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "event_user_is_anonymous", "value": "1" },
      { "type": "selector", "dimension": "event_user_is_bot_by_group", "value": "0" },
      { "type": "selector", "dimension": "event_user_is_bot_by_name", "value": "0" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "modified_bytes", "fieldName": "text_bytes_diff_abs" }
  ],
  "intervals": [ "2016-03-01T00:00:00.000/2016-04-01T00:00:00.000" ]
}
' http://druid1001.eqiad.wmnet:8082/druid/v2/
```


## For later

Metrics that updates from the beginning of time. For instance total number of articles
or total number of editors, computed from beginning of time for every new month.
Druid doesn't compute those metrics as is, but we can precompute them at warmup:
We can compute total number of articles since beginning of time for every month,
and later have AQS query and cache those values.



