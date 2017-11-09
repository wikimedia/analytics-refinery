#!/bin/bash

set -e
set -o pipefail

error() {
    echo "Error:" "$@" >&2
    exit 1
}

print_usage() {
    cat <<EOF
$0 SOURCE_HDFS_PATH_ABS EXPECTED_ENDING DONE_FLAG

identifies the content file in an HDFS directory and echos the found
name to stdout in Java Property file format for the property
'content_file'.

SOURCE_HDFS_PATH_ABS -- The directory holding the file to archive
EXPECTED_ENDING      -- The content file is expected to end in this string
DONE_FLAG            -- The done flag in SOURCE_HDFS_PATH_ABS.
                        The script assumes that the directory contains
                        the done flag, but the presence of the done
                        flag is not actually checked by this
                        script. This parameter is only used to exclude
                        it as content_file.
EOF
}


while [ $# -gt 0 ]; do
    PARAM="$1"
    case "$PARAM" in
        "--help")
            print_usage
            exit 0
            ;;
        *)
            break
            ;;
    esac
    shift
done

SOURCE_HDFS_PATH_ABS="$1"
EXPECTED_ENDING="$2"
DONE_FLAG="$3"

if [ $# -ne 3 ]
then
    print_usage
    error "You must provide SOURCE_HDFS_PATH_ABS, EXPECTED_ENDING, and DONE_FLAG"
fi

if [ "${SOURCE_HDFS_PATH_ABS:0:7}" != "hdfs://" ]
then
    error "Source path '$SOURCE_HDFS_PATH_ABS' does not start in 'hdfs://'"
fi

HDFS_CONTENTS=$(
    hdfs dfs -ls "$SOURCE_HDFS_PATH_ABS" 2> >(grep -v '^SLF4J' >&2 || true) \
        | (grep -v ^Found || true) \
        | sed -e 's@^.*/\([^/]*\)$@\1@' )

if [ "2" -lt "$(wc -l <<<"$HDFS_CONTENTS")" ]
then
    error "Source path '$SOURCE_HDFS_PATH_ABS' contains more than 2 files"
fi

CONTENTS_FILE_RELS=
while read FILE_RELS
do
    if [ "$FILE_RELS" != "$DONE_FLAG" ]
    then
        if [ "${FILE_RELS:$((${#FILE_RELS}-${#EXPECTED_ENDING}))}" = "$EXPECTED_ENDING" ]
        then
            CONTENTS_FILE_RELS="$FILE_RELS"
        else
            error "The file '$SOURCE_HDFS_PATH_ABS/$FILE_RELS' does not end in '$EXPECTED_ENDING'"
        fi
    fi
done <<<"$HDFS_CONTENTS"

if [ -z "$CONTENTS_FILE_RELS" ]
then
    # This should never happen ... but just in case
    error "Could not find content file in source path '$SOURCE_HDFS_PATH_ABS'"
fi

echo "content_file=$CONTENTS_FILE_RELS"
