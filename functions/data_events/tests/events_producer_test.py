import os
import sys
import pytest
import psycopg2

fileDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.dirname(fileDir)

sys.path.append(parentDir)

from dotenv import load_dotenv
load_dotenv()

from events_producer import update


def test_producer():
    event = {
    "Records": [
        {
            "Sns": {
                "Message": "{\"account_id\": 10, \"account\": \"propair-bank\", \"start_date\": \"03/01/2020 00:00:00\", \"end_date\": \"03/19/2020 00:00:00\", \"velocify_username\": \"velocifysandbox@pro-pair.com\", \"velocify_password\": \"df*ejsh#w9kjnDsQm!!akqi\", \"fromNowMinutes\": 30}"
            }
        }
    ]
}
    assert update(event, {}) == None