Oozie workflows in this directory run Hive actions. The workflows
here should be generic enough to run on their own via good
parameterization.  I.e. they should not refer to specific tables
or data in HDFS.

hive-site.xml is a symlink to /etc/hive/conf/hive-site.xml.  If you
run ```hdfs dfs -put``` for the whole oozie/ directory, hive-site.xml
will be put as the full text of the original file in HDFS.
This file is referenced from various files relative to an ```${oozieDirectory}```
property.
