# Mediawiki History Metrics using Druid

This document present the metrics we compute in Druid and serve through AQS.

Those metrics use two types of druid queries, timeseries and topN, and this document
uses this natural split to present them.


******************


## Metrics Definitions


### Timeseries metrics

Data points over a single measure -- time interval not limited -- daily or monthly
time granularities -- with filters (project, page-type, editor-type, activity-level)


* new articles
  * Formula: Count page-create events (by construction no deleted pages)
  * Filters: project, page-type


* edited articles
  * Formula: count distinct article with at least one edit
  * Filters: project, page-type, editor-type, activity-level


* new users
  * Formula: user create (created by self, to prevent auto-create)
  * Filters: project


* editors
  * Formula: count distinct user with an edit
  * Filters: project, user-type, page-type, activity-level


* edits
  * Formula: count edits
  * Filters: project, user-type, page-type


* added bytes
  * Formula: sum of text_bytes_diff (positive and negative values, result can be 0)
  * Filters: project, user-type, page-type


* modified bytes
  * Formula: sum of absolute value of text_bytes_diff
  * Filters: project, user-type, page-type


### TopN metrics

Top 100 over a single measure -- interval limited to one of the chosen time
granularity (daily or monthly) -- with filters


* Most edited articles
  * Formula: top 100 page_title by number of edits
  * Filters: project, page-type


* Articles with most contributors
  * Formula: top 100 page_title by number of distinct user_id
   (to mitigate anonymous)
  * Filters: project, page-type


* Articles with largest growth
  * Formula: top 100 page_title by sum of text_bytes_diff
  * Filters: project, page-type


* Articles most modified (bytes)
  * Formula: top 100 page_title by sum of text_bytes_diff_abs
  * Filters: project, page-type


* Contributors with most edits
  * Formula: top 100 user_text by number of edits
  * Filters: project, user-type


* Contributors having added most bytes
  * Formula: top 100 user_text by sum of text_bytes_diff
  * Filters: project, user-type


* Contributors having modified most bytes
  * Formula: top 100 user_text by sum of text_bytes_diff_abs
  * Filters: project, user-type


## Implementation


### Problems and Solutions


#### The **additivity** issue

One of the concern of OLAP systems like Druid that allow for aggregations of numerical data among
dimensions (also known as *slicing and dicing*) is to make sure that metric definition
allow for the aforementioned aggregations. To keep it simple and close to our real use-case,
we will talk of the **additivity** problem (it becomes even trickier if you use aggregations
like max, min etc instead of sum).

While it's intuitive and natural to think of aggregations of additive metrics, it is
usually less evident (at least for me) to spot a non-additive metric.
Let's take examples:
 1 Number of edits over a time period (day or month), broken-down or rolled-up by project.
   This metric is additive: if we have the number of daily-edits for every-day of a month,
   we sum them up and have the value for the given month. Same for projects, if we have values
   for every project, their sum is the global value.
 2 Number of editors over a time period (day or month), broken-down or rolled-up by project.
   This metric however is non-additive: if we have the number of editors for every day of a month
   and some them up, there are many chances that the result will be different from the computation
   of distinct users having done at least one edit for that month. This is due to the fact that if
   an editor makes one edit a day for every day of a month, it would count as 1 in every daily
   metric value, and also 1 in the monthly computation (and not 31).

In a general way, non-additive metrics are the ones defined as *distinct count*. For those, either
you have events that by construction enforce the *distinct* aspect over your dimensions (such as
`edit` for us), either you need to compute a **distinct count** over your dimensions and values
(see next session).


#### The **distinct count** issue

As stated in the previous section, when you have rows that by construction enforce the *distinct*
aspect over your dimensions, counting distinct is as simple as counting those rows (`edits`
in our use-case). When your data isn't formatted in such a way, you need to make sure every item
you want distinct appears only once. This is equivalent to doing a `group by` the key you want
distinct, then count the rows. While easy to say, it is actually tricky to do at scale and fast.

For use-cases that can afford a bit of imprecision, Druid implements the HyperLogLog hashing trick
to compute fast and at scale an estimation of the *count distinct*, with estimated error being
about 2% (tested with our datase). This solution will be very usefull for some use-cases we have,
such as ranking by distinct-count as in *articles with most conributors* metric, where the actual
count value can afford some imprecision as long as the ranking is correct. However the 2% error is
too big for the core metrics needing distinct-count: *editors* and *edited articles*. We also tried
to use the `group by` then `count` strategy, but it doesn't scale correctly in Druid, as it keeps
every resulting row of the inner query in memory.


#### The data-massaging solution

The solution we picked for solving the two issues above is to prepare (massage) our data to fit our
queries use-cases, and be carefull with how we query the dataset. While not being really generic
nor intuitive in term of usage, this trick allows us to make Druid answer our queries fast and
with correct results. The down-side is that it needs us to do heavier precomputation, and that our
dataset in Druid contains more data, as it not only contains raw events but prepared ones.

In detail, we added rows with special type (to be filtered out of regular queries), that are unique
by eiher page_title (including namespace prefix) or user_text, time granularity (day or month),
and any of the needed split-dimension (page-type and editor-type). We took advantage of having such
new rows to store the count of edits for that period and dimension setting, named *activity level*,
for even more filtering capacity. Last concern, this trick requires us to store events for the
aggregated versions of our dimensions splits `page-type` and `editor-type`; we don't deduplicate
users accross projects, so our distinct-count metrics are additive over that dimension. Instead of
generating new fields for each of the possible value pairs, we encode the two dimensions values
as a string, and use a special value "all" for precomputed disinct rows.


### Data Schema


Field | Type | Comment
--- | --- | ---
`project`                               | `string` | The project hostname -- Warning: We don't deduplicate users accross projects(the same username in two different projects arecounted as different users)
`event_entity`                          | `string` | The entity the event is related to. Can be `revision`, `page` or `user` (This field should **always** be filtered on when querying to have correct result)
`event_type`                            | `string` | The type of event - Can be `create`, `delete, `restore`, `daily_digest` or `monthly_digest` (the last two are special events used for distinct count)
`event_timestamp`                       | `string` | The timestamp of the event in SQL format (YYYY-MM-DD HH:MM:SS.0)
`user_text`                             | `string` | user_text of the performing user (IP if anonymous)
`user_type`                             | `string` | Can be `anonymous`, `group_bot` (user is in the bot group), `name_bot` (user is not on the bot group but has a name that looks like a bot) or `user`. The special `all` value is used with `*ly_digest` event_types to gather `activity_lelve` for the rooled up dimension
`page_title`                            | `string` | The page_title the event applies to (prefixed with its canonical namespace)
`page_namespace`                        | `int`    | The page_namespace the event applies to (for this one we keep historified values)
`page_type`                             | `string` | Can be `content` (the page belongs in a content namespace) or `non_content` (the page belongs in a non-content namespace). The special `all` value is used with `*ly_digest` event_types to gather `activity_level` for the rolled up dimension
`other_tags`                            | `array<string>` | Contains flags about the revisions or users events, allowing us to prevent keeping them as single dimensions. Can contain `user_first_24_hours` (if the revision happens within 24h of the performer creation), `redirect` (if the revision belongs to a page that is a redirect), `deleted` (if the revision has been deleted), `deleted_day`/`deleted_month`/`deleted_year` (if the revision has been deleted the same day, month or year), `reverted` or `revert` if the revision has been reverted or is a revert, `reverted_X` where `X` is a time period in `minute`, `5_minutes`, `10_minutes`, `hour`, `12_hours`, `day`, `3_days`, `week`, `2_weeks`, `month`, `3_month`, `6_month`, `year`, and finally `self_created`/`system_created`/`peer_created` if a user has been created by himself, a peer or the autologin system.
`text_bytes_diff`                       | `bigint` | The sum of differences in bytes between previous revisions and current new ones (positive or negative)
`text_bytes_diff_abs`                   | `bigint` | The sum of absolute value of the differences in bytes between previous revisions and current new one (always positive or null)
`revisions`                             | `bigint` | Number of revisions - 1 for revision, 0 for other events except digests, where it gets the sum of revisions for the given digest - used to filter count-distinct by more precise activity level measure (count distinct users having made at least 5 edits this month, for instance)



## Druid examples


Can be run as is from any stat100X machine.


### Timeseries metrics


* new articles -- Daily, whole 2016, en.wikipedia, content only
  * Time - NOT cached:  255ms
  * Time - Cached:       49ms
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
      { "type": "selector", "dimension": "page_type", "value": "content" }
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
            "name" : "articles",
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
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* edited articles -- Daily, january 2016, en.wikipedia, group-bot only, content only, page-month-activity-level more than 5 included
  * Time - NOT cached:   59ms
  * Time - Cached:       24ms
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
      { "type": "selector", "dimension": "event_type", "value": "daily_digest" },
      { "type": "selector", "dimension": "page_type", "value": "content" },
      { "type": "selector", "dimension": "user_type", "value": "group_bot" },
      { "type": "regex", "dimension": "revisions", "pattern": "^([56789]|\\d{1}\\d+)$" }
    ]
  },
  "aggregations": [
      { "type": "longSum", "name": "articles", "fieldName": "events" }
    ],
  "intervals": [ "2016-01-01T00:00:00.000/2016-02-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* new users -- Monthly, whole 2016, en.wikipedia, users only
  * Time - NOT cached:   134ms
  * Time - Cached:        24ms
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
      { "type": "selector", "dimension": "user_type", "value": "user" }
    ]
  },
  "aggregations": [
      { "type": "longSum", "name": "users", "fieldName": "events" }
    ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* editors -- Monthly, whole 2016, en.wikipedia, anonymous only, any page-type, user-activity-level between 5 and 99 included
  * Time - NOT cached:    62ms
  * Time - Cached:        38ms
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
      { "type": "selector", "dimension": "event_type", "value": "monthly_digest" },
      { "type": "selector", "dimension": "user_type", "value": "anonymous" },
      { "type": "selector", "dimension": "page_type", "value": "all" },
      { "type": "regex", "dimension": "revisions", "pattern": "^([56789]|\\d{2})$" }
    ]
  },
  "aggregations": [
      { "type": "longSum", "name": "users", "fieldName": "events" }
    ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* edits -- Daily, whole 2016, en.wikipedia, user only, non-content only
  * Time - NOT cached:   432ms
  * Time - Cached:        35ms
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
      { "type": "selector", "dimension": "user_type", "value": "user" },
      { "type": "selector", "dimension": "page_type", "value": "non_content" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "edits", "fieldName": "events" }
  ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* added bytes -- Daily, whole 2016, en.wikipedia, anonymous only, non-content only
  * Time - NOT cached:   295ms
  * Time - Cached:        35ms
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
      { "type": "selector", "dimension": "user_type", "value": "anonymous" },
      { "type": "selector", "dimension": "page_type", "value": "non_content" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "added_bytes", "fieldName": "text_bytes_diff" }
  ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* modified bytes -- Monthly, whole 2016, en.wikipedia, any user-type, content only
  * Time - NOT cached:   335ms
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
      { "type": "selector", "dimension": "page_type", "value": "content" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "modified_bytes", "fieldName": "text_bytes_diff_abs" }
  ],
  "intervals": [ "2016-01-01T00:00:00.000/2017-01-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```

### TopN Metrics


* Most edited articles -- Monthly, 2017-07, en.wikipedia, content type, user
  * Time - NOT cached:  1126ms
  * Time - Cached:        26ms
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
      { "type": "selector", "dimension": "user_type", "value": "user" },
      { "type": "selector", "dimension": "page_type", "value": "content" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "edits", "fieldName": "events" }
  ],
  "intervals": [ "2017-07-01T00:00:00.000/2017-08-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* Articles with most contributors -- Monthly, 2017-06, en.wikipedia, non-content type, user

  * Time - NOT cached:  3357
  * Time - Cached:        33ms
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
      { "type": "selector", "dimension": "user_type", "value": "user" },
      { "type": "selector", "dimension": "page_type", "value": "non_content" }
    ]
  },
  "aggregations": [
    {
      "type": "cardinality",
      "name": "contributors",
      "fields": [ "user_text" ]
    }
  ],
  "intervals": [ "2017-06-01T00:00:00.000/2017-07-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* Articles with largest growth -- Monthly, 2017-05, en.wikipedia, content type, group-bot
  * Time - NOT cached:  638ms
  * Time - Cached:       28ms
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
      { "type": "selector", "dimension": "user_type", "value": "group_bot" },
      { "type": "selector", "dimension": "page_type", "value": "content" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "added_bytes", "fieldName": "text_bytes_diff" }
  ],
  "intervals": [ "2017-05-01T00:00:00.000/2017-06-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* Articles most modified (bytes) -- Monthly, 2017-04, en.wikipedia, non-content type, any editor-type
  * Time - NOT cached:   688ms
  * Time - Cached:        28ms
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
      { "type": "selector", "dimension": "page_type", "value": "non_content" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "modified_bytes", "fieldName": "text_bytes_diff_abs" }
  ],
  "intervals": [ "2017-04-01T00:00:00.000/2017-05-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* Contributors with most edits -- Monthly, 2017-07, en.wikipedia, user type, content
  * Time - NOT cached:   351ms
  * Time - Cached:        27ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "month",
  "dimension": "user_text",
  "metric": "edits",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "user_type", "value": "user" },
      { "type": "selector", "dimension": "page_type", "value": "content" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "edits", "fieldName": "events" }
  ],
  "intervals": [ "2017-07-01T00:00:00.000/2017-08-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* Contributors having added most bytes -- Daily, 2017-07-27, en.wikipedia, bot by group type, any page-type
  * Time - NOT cached:   136ms
  * Time - Cached:        27ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "day",
  "dimension": "user_text",
  "metric": "added_bytes",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "user_type", "value": "group_bot" }
  },
  "aggregations": [
    { "type": "longSum", "name": "added_bytes", "fieldName": "text_bytes_diff" }
  ],
  "intervals": [ "2017-07-27T00:00:00.000/2017-07-28T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


* Contributors having modified most bytes -- Monthly, 2016-03, en.wikipedia, anonymous, non-content
  * Time - NOT cached:   551ms
  * Time - Cached:        58ms
  * Query:
```
time curl -L -H'Content-Type: application/json' -XPOST --data-binary '
{
  "queryType": "topN",
  "dataSource": "mediawiki_history_reduced",
  "granularity": "day",
  "dimension": "user_text",
  "metric": "modified_bytes",
  "threshold": 100,
  "filter": {
    "type": "and",
    "fields": [
      { "type": "selector", "dimension": "project", "value": "en.wikipedia" },
      { "type": "selector", "dimension": "event_entity", "value": "revision" },
      { "type": "selector", "dimension": "event_type", "value": "create" },
      { "type": "selector", "dimension": "user_type", "value": "anonymous" },
      { "type": "selector", "dimension": "page_type", "value": "non_content" }
    ]
  },
  "aggregations": [
    { "type": "longSum", "name": "modified_bytes", "fieldName": "text_bytes_diff_abs" }
  ],
  "intervals": [ "2016-03-01T00:00:00.000/2016-04-01T00:00:00.000" ]
}
' http://druid1004.eqiad.wmnet:8082/druid/v2/
```


## For later

### Deletion Drift

For the moment we don't measures deleted revisions. They are part of existing data
and we don't impact the metrics we compute with them. The original version of wikistats
took those deletion into account by construction (XML dumps don't contain deleted
revisions). We'd like to be able to provide metrics with and without deletion drift,
for all metrics.

### Whole times metrics

Metrics that updates from the beginning of time. For instance total number of articles
or total number of editors, computed from beginning of time for every new month.
Druid doesn't compute those metrics as is, but we can precompute them at warmup:
We can compute total number of articles since beginning of time for every month,
and later have AQS query and cache those values.



