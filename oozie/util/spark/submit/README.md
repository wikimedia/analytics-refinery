This is a workflow intended to be used to submit Spark jobs via an Oozie shell action.
Oozie has a built in Spark action, but it does not work with HiveContext due to classpath issues.
This workflow can be used to submit Spark jobs that use HiveContext by bypassing whatever
Oozie classpath issues cause the problem, and submitting a Spark job directly with spark-submit.

This workflow expects Spark/YARN to support dynamic allocation, and will use it by default.
You can configure the maximum number of allocated executors by setting the spark_max_executors
property.  spark_max_executors defaults to 64.

This workflow expects that your Spark job is configured to intake its CLI options as a single
string passed to the --options flag.  This allows us to use a single workflow with variable
Spark app CLI options.  Oozie requires that individual CLI options are each specified in their
own <argument></argument> tag, which is inflexble on its own if we want to work with any
generic Spark job.
