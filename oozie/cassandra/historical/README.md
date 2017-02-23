# Cassandra loader for historical data (one-off)

This folder contains some files to upload historical data to
cassandra. As opposed to regular data, historical data needs
to be loaded only once, thus doesn't need a coordinator, nor
being part of a bundle, only a workflow.

The workflow is intended to be generic for all historical data.
In addition to that, there should be one .hql file and one
.properties file for each data set to be uploaded to cassandra.

Note: When developing new .hql files for new historical jobs,
take into account that the generic workflow file passes the
following parameters to the .hql code:
* destination_directory  The directory where to output results.
* source_table_1  Main table from where to extract results.
* source_table_2  Secondary table from where to extract results.
* separator  Separator character for the output.
If the new hql query only has 1 source table, you can omit
source_table_2 in the .properties file. If it needs to query
more than 2 tables, consider adding source_table_3 to the
workflow.
