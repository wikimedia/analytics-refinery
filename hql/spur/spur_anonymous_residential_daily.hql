-- Import last release of anonymous residential SPUR data from JSON files into an Iceberg table.
--
-- The Anonymous+Residential data feed includes all IP addresses in the Anonymous data feed,
-- plus IPs contributing to residential proxies, malware proxies, peer-to-peer proxies,
-- blockchain proxies, and ZTNA infrastructure. Each feed line is an IP Context Object,
-- indexed by IPv4 or IPv6 Address.
--
-- Parameters:
--     destination_table -- Fully qualified table name to fill
--     day               -- day string YYYY-MM-DD
--     output_partitions -- Number of output files
--     input_json_path   -- Path to input JSON file
--
-- Usage:
--     spark3-sql -f spur_anonymous_residential_daily.hql \
--         -d destination_table=wmf_traffic.spur_anonymous_residential \
--         -d output_partitions=12 \
--         -d input_json_path=hdfs:///wmf/data/wmf_traffic/spur/raw/anonymous_residential/2025-11-24/  \
--         -d day=2025-11-30

DELETE FROM ${destination_table}
WHERE `day` = DATE('${day}');

INSERT INTO TABLE ${destination_table}
SELECT /*+ REPARTITION(${output_partitions}) */
    ip,
    organization,
    `as`,
    client,
    tunnels,
    services,
    location,
    risks,
    infrastructure,
    DATE('${day}') AS day
FROM json.`${input_json_path}`;
