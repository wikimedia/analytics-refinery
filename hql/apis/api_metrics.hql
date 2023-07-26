--Generates metrics for API (restbase and action)
--
--This hql script is used as input to a HiveToGraphite spark job
--which was created to replace jobs such APIsVarnishRequests.scala job.
--The APIsVarnishRequests.scala job extracts data from hive,
--and then reformats and sends them to graphite.
--
--Using the new HiveToGraphite job, the API metric data to be sent to graphite
--would be generated purely by this HQL script and not through reformatting/
--re-processing.
--
--How the query works:
--      Creates a map with the metric-name as key and count as value
--      Next explode the map into rows
--
-- Parameters:
--     source_table         -- The web request table to query.
--     year                 -- year of API to compute metric for.
--     month                -- month of API to compute metric for.
--     day                  -- day of API to compute metric for.
--     hour                 -- hour of API to compute metric for.
--
-- Usage:
--     spark3-sql --master yarn -f api_metrics.hql                                  \
--         -d source_table = wmf.webrequest                                         \
--         -d year=2021                                                             \
--         -d month=4                                                               \
--         -d day=3                                                                 \
--         -d hour=6
--

WITH
  metricmap AS (
  SELECT
    MAP (
      'restbase.requests.varnish_requests', SUM(CASE WHEN uri_path like '/api/rest_v1%' THEN 1 ELSE 0 END),
      'analytics.mw_api.varnish_requests', SUM(CASE WHEN uri_path like '/w/api.php%' THEN 1 ELSE 0 END)
    ) as values_map
  FROM ${source_table}
  WHERE webrequest_source = 'text'
    AND year = ${year}
    AND month = ${month}
    AND day = ${day}
    AND hour = ${hour}
  )
SELECT
  EXPLODE(values_map) as (metric_id, request_count),
  --The year, month, day and hour fields are not zero padded here because CAST timestamp parsing
  --works fine without zero(0) padding.
  CAST('${year}-${month}-${day} ${hour}:00:00' as timestamp ) as time_id
FROM metricmap
