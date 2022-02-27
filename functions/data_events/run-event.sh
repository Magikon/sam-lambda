#!/usr/bin/env bash

set -e 

source .python/bin/activate

export DB_ENDPOINT="staging.c6pocrcpqs8o.us-west-1.rds.amazonaws.com"
export DB_NAME="propair_staging"
export DB_USER="postgres"
export ENV="staging"
export BUCKET="pp-tables"

FILE_COUNT=0
ACCOUNT_ID=10
START_DATE="02/01/2019 00:00:00"
END_DATE=$(date -d "${START_DATE} EDT + 7 hours" '+%m/%d/%Y %T')

until [[ $END_DATE > 05/01/2019 ]]; do
    FILE_COUNT=$((FILE_COUNT+1))
    echo $FILE_COUNT
    echo $START_DATE " to " $END_DATE

    JSON_STRING=$( jq -n \
                  --arg a $ACCOUNT_ID \
                  --arg b "$START_DATE" \
                  --arg c "$END_DATE" \
                  '{account_id: $a, start_date: $b, end_date: $c}' )

    echo $JSON_STRING > event.json

    python3 event_pull.py

    START_DATE=$(date -d "${START_DATE} EDT + 7 hours" '+%m/%d/%Y %T')
    END_DATE=$(date -d "${END_DATE} EDT + 7 hours" '+%m/%d/%Y %T')

done
