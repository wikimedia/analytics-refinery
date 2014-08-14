#!/bin/bash

set -e

usage="Usage: $(basename "$0") [--dry-run|-n] [--hostname|-h <icinga_reported_hostname>] <icinga_service_description> <dataset_location>"

icinga_nsca_host='icinga.wikimedia.org'
icinga_nsca_port='5667'

# This is hardcoded, as any DataNode may run this script
# and we want the service to always be associated with
# the same node.
icinga_reported_hostname='analytics1027'

dry_run='false'

# Parse CLI args
while [ $# -gt 2 ]; do
    case "$1" in
    -n|--dry-run)
        dry_run='true'
        ;;
    -h|--hostname)
        shift
        icinga_reported_hostname="$1"
        ;;
    *)
        break
        ;;
    esac
    shift
done

icinga_service_description="$1"
location="$2"

if [ -z "${icinga_service_description}" -o -z "${location}" ]; then
    echo "Error: Must provide <icinga_service_description> and <location>."
    echo "${usage}"
    exit 3
fi

nsca_message="${icinga_reported_hostname}	${icinga_service_description}	0	OK: A dataset has recently become ready. Location: ${location}"

if [ "${dry_run}" == 'true' ]; then
    echo 'Dry run. Not sending passive check to icinga.'
    echo "${nsca_message}"
else
    echo "${nsca_message}" | /usr/sbin/send_nsca -H "${icinga_nsca_host}" -p "${icinga_nsca_port}"
fi
