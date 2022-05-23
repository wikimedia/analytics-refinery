TODO Deprecated, this job has been migrated to Airflow.

Oozie job to schedule generating clickstream datasets for various projects.
The job runs every month and its TSV results are synchronised to the public.

The oozie workflow launches a spark action that runs the
ClickstreamBuilder scala job in analytics-refinery-source/refinery-job and
saves results in a temporary folder.
Then a loop is made to archive files from the temporary folder to the archive
one, with nice names.
