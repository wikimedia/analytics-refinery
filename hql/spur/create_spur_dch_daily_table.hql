-- The Datacenter Hosting feed (DCH) is a netblock-organized feed of all IPv4 and IPv6 addresses
-- belonging to datacenter infrastructure. These networks are specifically for leased or cloud
-- infrastructure, typically in a hosting or server facility. If user traffic is observed from
-- these networks, it is likely distinct from the physical location of the IP.

CREATE TABLE IF NOT EXISTS wmf_traffic.spur_dch
(
    asn bigint COMMENT 'Autonomous System Number (ASN) announcing the IP prefix. Identifies the network operator responsible for routing the datacenter range via BGP.',
    network string COMMENT 'CIDR-formatted IP network range (e.g., 34.32.0.0/11) associated with the datacenter block as classified by Spur. ex: 34.32.0.0/11',
    organization string COMMENT 'Organization that owns or operates the datacenter IP range (e.g., cloud providers, hosting companies, colocation facilities) ex: Google LLC',
    `day` DATE COMMENT 'Date of the Spur release'
)
USING ICEBERG
PARTITIONED BY (`day`)
TBLPROPERTIES (
                  'format-version' = '2',
                  'write.delete.mode' = 'copy-on-write',
                  'write.parquet.compression-codec' = 'zstd',
                  'write.distribution-mode' = 'hash'
              )
COMMENT 'Spur Datacenter feed mapping each datacenter IP prefix to its ASN and operating organization, used to identify cloud, hosting, and server-originated traffic. Updated daily.'
LOCATION 'hdfs://analytics-hadoop/wmf/data/wmf_traffic/spur/dch';
