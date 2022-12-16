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

# Convert FSImage to XML
# ======================
#
# * fetch an FSImage from HDFS
# * convert the image to XML using hdfs oiv tool
# * send the XML version of the image back to HDFS
#
# The FSImage weights ~10GB in raw, and ~40GB in XML (in 2022). So this scripts
# assumes there is enough temporary disk space available on the worker.
#
# The compressed raw FSImage is currently kept. Cleaning the FSImages will be the subject of another job.

set -e
set -x

if [[ ! ( "$#" == 2 ) ]] ; then
  echo "Please pass 2 arguments to this script:"
  echo "  - raw_fsimage (eg: /wmf/data/raw/hdfs/fsimage/fsimage_2022-12-01.gz)"
  echo "  - raw_xml_file (eg: /wmf/data/raw/hdfs/xml_fsimage/fsimage_2022-12-01.xml)"
  exit 1
fi

raw_fsimage="$1"
local_raw_fsimage="$(basename $raw_fsimage)"  # eg: fsimage_2022-12-01.gz
local_raw_fsimage_uncompressed="$(basename -s .gz $raw_fsimage)"  # eg: fsimage_2022-12-01
raw_xml_file="$2"
local_raw_xml_file="$(basename $raw_xml_file)"  # eg: fsimage_2022-12-01.xml

if ! hdfs dfs -test -e "${raw_fsimage}" ; then
    echo "Missing source file on HDFS ${raw_fsimage}"
    exit 1
fi

echo "Fetching FSImage from HDFS: ${local_raw_fsimage}"
hdfs dfs -get $raw_fsimage $local_raw_fsimage

echo "Uncompressing raw FSImage: ${local_raw_fsimage}"
gzip --decompress $local_raw_fsimage

echo "Converting FSImage to XML:"
echo "  ${local_raw_fsimage_uncompressed} => ${local_raw_xml_file}"
hdfs oiv -i $local_raw_fsimage_uncompressed -o $local_raw_xml_file -p XML

# log image sizes
ls -lh "${local_raw_fsimage_uncompressed}" "${local_raw_xml_file}"

echo "Sending FSImage as XML to HDFS"
if hdfs dfs -test -e "${raw_xml_file}" ; then
    echo "Overriding ${raw_xml_file} on HDFS."
fi
hdfs dfs -put -f "${local_raw_xml_file}" "${raw_xml_file}"

set +e
set +x

echo 'Done.'
