-- The Anonymous+Residential data feed includes all IP addresses in the Anonymous data feed,
-- plus IPs contributing to residential proxies, malware proxies, peer-to-peer proxies,
-- blockchain proxies, and ZTNA infrastructure. Each feed line is an IP Context Object,
-- indexed by IPv4 or IPv6 Address.

CREATE TABLE IF NOT EXISTS wmf_traffic.spur_anonymous_residential
(
    ip string COMMENT 'IPv4 address associated with the connection, as reported in the Spur IP Context feed',
    organization string COMMENT 'The organization operating this IP address',
    `as` struct<
        number:bigint COMMENT 'The autonomous system number',
        organization:string COMMENT 'The organization that owns and operates the Autonomous System (AS) responsible for routing the IP address via BGP'
    > COMMENT 'Spur IP Geo/BGP autonomous system information for the IP',
    client struct<
        behaviors:array<string> COMMENT 'Observed behavioral classifications of clients behind this IP ex: [FILE_SHARING, TOR_PROXY_USER]',
        concentration:struct<
            city:string COMMENT 'City of highest observed client concentration',
            country:string COMMENT 'Country of highest observed client concentration ex: IN',
            density:double COMMENT 'The density defines the percentage of clients on this IP observed in this concentrated location, versus other locations. Density can be 0-1.',
            geohash:string COMMENT 'Geohash representation of primary client concentration region',
            skew:bigint COMMENT 'The skew field defines the straight line distance in kilometers from the top level IP location.',
            state:string COMMENT 'State or region of highest observed client concentration ex: Madhya Pradesh'
        > COMMENT 'The strongest location concentration for clients using this IP address. The size of the concentration is determined by the length (accuracy) of the geohash.',
        count:bigint COMMENT 'Average number of clients observed daily on this IP address',
        countries:bigint COMMENT 'Number of countries represented among observed clients',
        proxies:array<string> COMMENT 'The different types of callback proxies observed on clients using this IP address. Customers of these proxy services can use this IP address as their own by proxying through these client devices',
        spread:bigint COMMENT 'The total geographic area in kilometers where users are observed',
        types:array<string> COMMENT 'Types of clients behind the IP ex: [MOBILE]'
    > COMMENT 'Client description from Spur',
    tunnels array<
        struct<
            anonymous:boolean COMMENT 'If there is a reasonable suspicion that this VPN or tunnel is providing anonymous access. This is typically associated with no-log VPN providers.',
            entries:array<string> COMMENT 'IP addresses observed operating a tunnel that exits through this tunnel',
            exits:array<string> COMMENT 'IP addresses observed at the other end of this tunnel',
            `operator`:string COMMENT 'Name of the organization or service operating the tunnel',
            type:string COMMENT 'Tunnel type ex: VPN'
        >
    > COMMENT 'Tunneling information describing anonymizing services or proxy networks associated with this IP',
    services array<string> COMMENT 'Spur service tags and protocols associated with this IP ex: [OPENVPN]',
    location struct<
        city:string COMMENT 'City associated with the exit infrastructure of this IP, if available',
        state:string COMMENT 'State or region associated with the exit infrastructure of this IP, if available',
        country:string COMMENT 'Country associated with the exit infrastructure of this IP. ISO-3166'
    > COMMENT 'Data-center or IP Hosting location based on MaxMind GeoLite. This location represents the location of IP routing infrastructure, not necessarily the actual user.',
    risks array<string> COMMENT 'Risk tags from Spur describing notable risk factors or behaviors ex: [CALLBACK_PROXY, TUNNEL, GEO_MISMATCH]',
    infrastructure string COMMENT 'The primary type of infrastructure this IP is supporting. Common tags are MOBILE and DATACENTER.',
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
COMMENT 'Spur Anonymous+Residential IP Context feed for VPNs, proxies, and other anonymizing infrastructure plus residential/device-based proxy traffic. Updated daily.'
LOCATION 'hdfs://analytics-hadoop/wmf/data/wmf_traffic/spur/anonymous_residential';
