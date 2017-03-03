-- Repair partitions on a table.
--
-- Parameters:
--     table           -- Fully qualified table name to repair partition.
--
-- Usage:
--     hive -f repair_partitions.hql \
--         -d table=wmf.webrequest
--

SET hive.mapred.mode = nonstrict;

MSCK REPAIR TABLE ${table};
