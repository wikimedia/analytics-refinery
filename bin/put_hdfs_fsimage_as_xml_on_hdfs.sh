#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Put XML fsimage on HDFS
# =======================
#
# * fetch an fsimage from the standby namenode with (requires HDFS superuser)
# * convert the image to XML using hdfs oiv tool
# * send the XML version of the image to HDFS
# * set the file ownership
#
# To fetch an fsimage from a name node, you need to be an HDFS superuser, or to
# fetch it from a namenode. As this script is supposed to be run from a Skein
# application on a random YARN worker, it needs to be run as a superuser.
#
# If you want to run this script manually, do it with the hdfs user (or any
# superuser).
#
# The fsimage weights ~10GB in raw, and ~40GB in XML (in 2022). So this scripts
# assumes there is enough temporary disk space available on the worker.

set -e
set -x

if [[ ! ( "$#" == 2 ) ]] ; then
  echo "Please pass 2 arguments to this script:"
  echo "  - raw_xml_data_directory (eg: /wmf/data/raw/hdfs_xml_fsimage)"
  echo "  - raw_xml_file (eg: 2022-10-26.xml)"
  exit 1
fi

raw_xml_data_directory="$1"
raw_xml_file="$2"

echo "Fetching fsimage from hdfs name server standby, store it locally..."
hdfs dfsadmin -fetchImage fsimage

echo "Converting fsimage to XML..."
hdfs oiv -i fsimage -o fsimage.xml -p XML

# log image sizes
ls -lh fsimage*

echo "Sending fsimage.xml to HDFS"
hdfs dfs -mkdir -p "${raw_xml_data_directory}"
# We keep the access to this dataset for admins only.
ownership="analytics:analytics-admins"
hdfs dfs -chown "${ownership}" "${raw_xml_data_directory}"
hdfs dfs -chmod 750 "${raw_xml_data_directory}"
hdfs_path="${raw_xml_data_directory}/${raw_xml_file}"
if hdfs dfs -test -e "${hdfs_path}" ; then
    echo "Overriding ${hdfs_path} on HDFS."
fi
hdfs dfs -put -f fsimage.xml "${hdfs_path}"
hdfs dfs -chown "${ownership}" "${hdfs_path}"
hdfs dfs -chmod 640 "${hdfs_path}"

set +e
set +x

echo 'Done.'
