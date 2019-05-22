# swift-upload workflow

This workflow takes a source directory and uploads it to an Openstack Swift objectstore.
It supports only Swift tempauth via authentication shell environment variables stored in
a env file.

Since the Hadoop Swift client doesn't support tempauth, this workflow runs a shell script
that downloads the source directory out of HDFS to a local temporary directory, and then
uploads it to Swift using the Swift CLI. By default, Swift objects will not be overwritten,
and the upload will fail if any object exists in the $swift_container prefixed with
$swift_object_prefix.  $should_overwrite can be set to true if you want to delete all
object with $swift_object_prefix before uploading.

