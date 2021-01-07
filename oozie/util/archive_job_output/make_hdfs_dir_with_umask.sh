#!/bin/bash

set -e
set -o pipefail

error() {
    echo "Error:" "$@" >&2
    exit 1
}

print_usage() {
    cat <<EOF
$0 HDFS_DIR_ABS UMASK

Creates an HDFS directory with a given umask.
Note: The directory is created only if it doesn't already exists.

HDFS_DIR_ABS  -- The directory to create
UMASK         -- The umask to use
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

if [ $# -ne 2 ]
then
    print_usage
    error "You must provide HDFS_DIR_ABS and UMASK"
fi

HDFS_DIR_ABS="$1"
UMASK="$2"

if [ "${HDFS_DIR_ABS:0:7}" != "hdfs://" ]
then
    error "Directory '$HDFS_DIR_ABS' does not start in 'hdfs://'"
fi

if ! [[ $UMASK =~ ^[0-7]{3}$ ]]
then
    error "Umask '$UMASK' is not valid"
fi

# If $HDFS_DIR_ABS already exists, its permissions are not changed
hdfs dfs -Dfs.permissions.umask-mode=$UMASK -mkdir -p $HDFS_DIR_ABS
