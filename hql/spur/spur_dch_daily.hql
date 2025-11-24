-- Import last release of DCH SPUR data from JSON files into an Iceberg table.
--
-- The Datacenter Hosting feed (DCH) is a netblock-organized feed of all IPv4 and IPv6 addresses
-- belonging to datacenter infrastructure. These networks are specifically for leased or cloud
-- infrastructure, typically in a hosting or server facility. If user traffic is observed from
-- these networks, it is likely distinct from the physical location of the IP.
--
-- Parameters:
--     destination_table -- Fully qualified table name to fill
--     day               -- day string YYYY-MM-DD
--     input_json_path   -- Path to input JSON file
--
-- Usage:
--     spark3-sql -f spur_dch_daily.hql \
--         -d destination_table=wmf_traffic.spur_dch \
--         -d input_json_path=hdfs:///wmf/data/wmf_traffic/spur/raw/dch/2025-11-24/ \
--         -d day=2025-11-30 \

DELETE FROM ${destination_table}
WHERE `day` = DATE('${day}');

INSERT INTO TABLE ${destination_table}
SELECT /*+ COALESCE(1) */
    asn,
    network,
    organization,
    DATE('${day}') AS day
FROM json.`${input_json_path}`;
