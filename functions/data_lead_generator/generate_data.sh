#!/usr/bin/env bash

# abort on error
set -e

export ENV="staging" 
export BUCKET="pro-pair-serverless-staging"
export REDSHIFT_ENDPOINT="propair-rs.cbwhpsk1qhvp.us-west-1.redshift.amazonaws.com" 
export REDSHIFT_DB_NAME="staging" 
export REDSHIFT_DB_USER="stageuser" 
export REDSHIFT_DB_PORT="5439" 

printf '\n'
printf '::: Generating Sythetic Data...\n'
printf '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'
printf 'Usage:\n --account <account_name>\n --days_ago <int>\n --lead-interval <int>\n'
printf '\nNote: Lead Interval defaults to 15 min, this means it will generate a lead in intervals of 15 min'
printf '\n'
printf '~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~\n'

ACCOUNT=""
DAYS_AGO=""
LEAD_INTERVAL="15"

while [[ $# > 0 ]] ; do
    case "$1" in
        -a|--account)
             if [ -z "${2}" ]
            then
                printf "[Error] Must specify an account name.\n"
                exit 1
            fi
            ACCOUNT=${2}
            shift
            ;;
        --days-ago)
            if [ -z "${2}" ]
            then
                printf "[Error] Must specify an integer for days-ago.\n"
                exit 1
            fi
            DAYS_AGO="${2}"
            shift
            ;;
        --lead-interval)
            if [ -z "${2}" ]
            then
                printf "[Error] Must specify an integer for lead-interval.\n"
                exit 1
            fi
            LEAD_INTERVAL="${2}"
            ;;
    esac
    shift
done 

if [ "${ACCOUNT}" == "" ]
then
    printf "[Error] Unable to find --account parameter\n"
    exit 1
fi

if [ "${DAYS_AGO}" == "" ]
then
    printf "[Error] Unable to find --days-ago parameter\n"
    exit 1
fi


source .python/bin/activate

#Generate Leads
ACCOUNT="${ACCOUNT}" DAYS_AGO="${DAYS_AGO}" LEAD_INTERVAL="${LEAD_INTERVAL}" python3 generate_leads.py

#Generate Events
ACCOUNT="${ACCOUNT}" DAYS_AGO="${DAYS_AGO}" python3 generate_events.py 

#Generate Calls
ACCOUNT="${ACCOUNT}" DAYS_AGO="${DAYS_AGO}" python3 generate_calls.py