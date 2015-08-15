# Todo:
* Move these to Puppet!

# webrequest_*
The webrequest_maps webrequest_mobile and webrequest_misc topics are relatively
small volume.  The webrequest-small camus job is expected to complete runs fairly quickly.

webrequest_text and webrequest_upload are both large volume.  These are
run as separate jobs so as not to interere with each other and smaller volume
webrequest imports.

The camus.webrequest.properties file contains all webrequest topics, but as
of 2015-08-15 is deprecated.