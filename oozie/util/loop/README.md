Downloaded from https://github.com/jeremybeard/oozieloop

This project helps you implement loops in Oozie by providing a workflow that will call your workflow for either each integer in a range you provide, or each entry in a list of values you provide. The execution of the workflows can be done in either serial order of the range or list, or in parallel.

The workflow that you call to run the loop is `loop.xml`. You can either run this directly or as a sub-workflow from another workflow that you may have.

Based on your desired type `loop.xml` will itself either run `loop_range_step.xml` recursively for each integer, or run `loop_list_step.xml` recursively for each list value, calling your workflow each time.

`loop.xml` requires these properties to process the loop:

- `loop_parallel` - Either "true" or "false". True will fork all of your workflows at once. False will wait for a workflow to finish before commencing the next.
- `loop_type` - Either “range” or “list”
- `loop_start` - The first integer to run in the loop (range type only)
- `loop_end` - The last integer to run in the loop (range type only)
- `loop_list` - The comma separated list of values to iterate over in the loop (list type only)
- `loop_action` - The HDFS path of the workflow XML that you need to loop over
- `loop_name` - A short name (without spaces) to distinguish the loop from others

When your workflow is called it will be passed the current value of the loop to the `loop_value` property, which you can use in your workflow. Note that due to limitations of Oozie, empty values in the list type will be provided instead as the value "--NOVALUE--".

The example folder in this project provides a demonstration that will create empty files in HDFS named for each value in the range or list that you provide. To run the example you will need to change the paths and hostnames to match your cluster.

By default this method can not loop over your workflow more than 48 times. This can be overridden at the Oozie server scope (beware!) by modifying `oozie.action.subworkflow.max.depth`.