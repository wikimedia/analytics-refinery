--
-- Load (set) the given property to cassandra's AQS config table.
--
-- NOTE: We've chosen to temporarily use Cassandra as a config store for AQS
--       as a convenient interim solution until the Dataset Config System
--       (https://phabricator.wikimedia.org/T354557) is available.
--
-- Parameters:
--     property_name         -- The property to configure.
--     property_value        -- The value to assign to the property.
--     aqs_config_table      -- Fully qualified name of AQS config table.
--
-- Usage:
--     spark3-sql \
--     --master local \
--     --conf spark.yarn.appMasterEnv.SPARK_CONF_DIR=/etc/spark3/conf \
--     --conf spark.yarn.appMasterEnv.SPARK_HOME=/usr/lib/spark3 \
--     --conf spark.sql.catalog.aqs=com.datastax.spark.connector.datasource.CassandraCatalog \
--     --conf spark.sql.catalog.aqs.spark.cassandra.connection.host=aqs1010-a.eqiad.wmnet:9042,aqs1011-a.eqiad.wmnet:9042,aqs1012-a.eqiad.wmnet:9042 \
--     --conf spark.sql.catalog.aqs.spark.cassandra.auth.username=aqsloader \
--     --conf spark.sql.catalog.aqs.spark.cassandra.auth.password=cassandra \
--     --conf spark.dynamicAllocation.maxExecutors=1 \
--     --conf spark.yarn.maxAppAttempts=1 \
--     --jars /srv/deployment/analytics/refinery/artifacts/org/wikimedia/analytics/refinery/refinery-job-0.2.17-shaded.jar  \
--     --driver-cores 1 \
--     --driver-memory 2G \
--     --executor-cores 1 \
--     --executor-memory 2G \
--     -f load_cassandra_aqs_config.hql \
--     -d property_name=mediawiki_history_reduced_druid_datasource \
--     -d property_value=mediawiki_history_reduced_2023_12 \
--     -d aqs_config_table=aqs.aqs.config
--

INSERT INTO ${aqs_config_table}
SELECT
    '${property_name}' AS param,
    '${property_value}' AS value
;
