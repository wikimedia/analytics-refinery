#!/bin/bash

#
# swift-upload
# Uploads a directory from HDFS or local filesystem into Swift.
# Uses Swift tempauth by sourcing environment variables from an auth file.
#
# <source_directory> will be uploaded to swift at <swift_object_prefix>.  Each file in the
# <source_directory> will be an object prefixed by <swift_object_prefix> and its relative path
# in the <source_directory>
#
# NOTE: The default storage-policy is 'lowlatency'.  Storage policies
# are only set per container, and cannot be changed after a container
# is created.  As such, the storage policy will only be respected
# if uploading to a new container for the first time.
#

# Exit on unset variable usage.
set -u

usage="Usage: $(basename $0) [-n|--dry-run] [-o|--overwrite <true|false>] -a|--auth-file <swift_auth_file> -c|--container <swift_container> [-s|--storage-policy <policy> (default: lowlatency)] <source_directory> <swift_object_prefix>"

# Logs a message to stdout
function log {
    echo -e "$(date --iso-8601=seconds)\t${@}"
}

# Logs a message to stderr and exits 1
function fatal {
    echo -e "$(date --iso-8601=seconds)\t${@}" >&2
    exit 1;
}

# Logs a command and runs it
function run_command {
    log "Running: ${@}"
    ${@}
    retval=$?
    return $retval
}


swift_auth_file=""
swift_container=""
# Default to using the 'lowlatency' SSD storage policy
swift_storage_policy="lowlatency"
dry_run="false"

# Default is to fail if the object prefix already exists in the container.
# If should_overwrite is true, all existent object with the prefix will
# be removed before uploading.
should_overwrite="false"

ARGS=""
while (( "$#" )); do
  case "$1" in
    -a|--auth-file)
        swift_auth_file="${2}"
        shift 2
        ;;
    -c|--container)
        swift_container="${2}"
        shift 2
        ;;
    -n|--dry-run)
        dry_run="true"
        shift 1
        ;;
    -s|--storage-policy)
        swift_storage_policy="${2}"
        shift 2
        ;;
    -o|--overwrite)
        should_overwrite="${2}"
        if [[ "${should_overwrite}" != "true" && "${should_overwrite}" != "false" ]]; then
            fatal "--overwrite must either be 'true' or 'false'\n${usage}"
        fi
        shift 2
        ;;
    -h|--help)
        echo "${usage}";
        exit 0;
      ;;
    -*|--*=) # unsupported flags
        fatal "Error: Unsupported flag $1.\n${usage}"
        ;;
    *) # preserve positional arguments
      ARGS="$ARGS $1"
      shift
      ;;
  esac
done

# set positional arguments in their proper place
eval set -- "$ARGS"


#
# -- Argument checking
#

if [ -z "${swift_container}" ]; then
    fatal "Error: Must provide --container argument.\n${usage}"
fi

if [ -z "${swift_auth_file}" ]; then
    fatal "Error: Must provide --swift_auth_file argument.\n${usage}"
fi

if [ ! -r "${swift_auth_file}" ]; then
    fatal "Error: --auth-file ${swift_auth_file} either does not exist or is not readable."
fi

source_directory="${1}"
swift_object_prefix="${2}"

if [ -z "${source_directory}" ]; then
    fatal "Error: Must provide <source_directory>.\n${usage}"
fi

if [ -z "${swift_object_prefix}" ]; then
    fatal "Error: Must provide <swift_object_prefix>.\n${usage}"
fi

log "Sourcing swift tempauth environment variables from ${swift_auth_file}."
source ${swift_auth_file}

if [[ -z "${ST_AUTH}" || -z "${ST_USER}" || -z "${ST_KEY}" ]]; then
    fatal "Error: ${swift_auth_file} must export ST_AUTH, ST_USER, and ST_KEY environment variables"
fi


# Check if the object prefix already exists
log "Checking if ${swift_object_prefix} already exists in Swift container ${swift_container}..."
existent_objects=$(swift list ${swift_container} | /bin/grep -E "^${swift_object_prefix}" | tr '\n', ' ')

# If there are existent objects with this prefix...
if [ -n "${existent_objects}" ]; then
    if [ "${dry_run}" == "false" ]; then
        # If should_overwrite, delete the object prefix and all its subobjects now
        if [ "${should_overwrite}" == "true" ]; then
            log "Deleting all objects in container ${swift_container} under prefix ${swift_object_prefix}..."
            run_command "swift delete ${swift_container} ${existent_objects}"
            if [ $? -ne 0 ]; then
                fatal "Error: Failed deleting all objects in container ${swift_container} under prefix ${swift_object_prefix}. Aborting."
            fi
        # Else error now.
        else
            fatal "Error: container ${swift_container} already has object(s) with prefix ${swift_object_prefix}. Aborting."
        fi
    else
        log "--dry-run mode, container ${swift_container} already has object(s) with prefix ${swift_object_prefix}. Ignoring."
    fi
fi


#
# -- Upload to Swift
#

log "Uploading ${source_directory} into Swift container ${swift_container} with object prefix ${swift_object_prefix}..."

# If we are uploading a directory from hdfs, first download it locally.
if [[ $source_directory == hdfs://* ]]; then

    temp_dir=$(/bin/mktemp -d -t "refinery-swift-upload-${swift_container}-XXXXXXXX")
    function remove_temp_dir {
        log "Removing temp dir ${temp_dir}"
        run_command rm -r "${temp_dir}"
    }
    trap remove_temp_dir EXIT

    if [ "${dry_run}" == "false" ]; then
        log "$source_directory is in HDFS. Downloading locally into ${temp_dir}/"
        run_command /usr/bin/hdfs dfs -get ${source_directory} ${temp_dir}/
    else
        log "--dry-run mode, $source_directory is in HDFS, would have downloaded locally into ${temp_dir}"
    fi

    upload_directory=${temp_dir}/$(basename "${source_directory}")
# Else assume this is a local filesystem path
else
    upload_directory="${source_directory}"
fi


swift_upload_command="/usr/bin/swift upload --header 'X-Storage-Policy:${swift_storage_policy}' --object-name ${swift_object_prefix} ${swift_container} ${upload_directory}"
swift_upload_retval=0

if [ "${dry_run}" == "false" ]; then
    log "Beginning Swift upload..."
    run_command $swift_upload_command
    # Save the retval for checking after potential removal of $temp_dir.
    swift_upload_retval=$?
else
    log "--dry-run mode, would have run: $swift_upload_command"
fi

# If the swift_upload_command ran and did not exit 0, exit now.
if [ $swift_upload_retval -ne 0 ]; then
    fatal "Error: Swift upload failed."
fi

# Verify that the object now exists in swift.
if [ "${dry_run}" == "false" ]; then
    log "Checking that ${swift_object_prefix} now exists in Swift container ${swift_container}..."
    swift list ${swift_container} | /bin/grep -qE "^${swift_object_prefix}"
    if [ $? -ne 0 ]; then
        fatal "Error: container ${swift_container} does not have object ${swift_object_prefix} after upload."
    else
        log "$source_directory successfully uploaded to container ${swift_container} as object ${swift_object_prefix}."
    fi
fi



