import os
import sys
import pytest
import psycopg2

fileDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.dirname(fileDir)

sys.path.append(parentDir)

from dotenv import load_dotenv
load_dotenv()

from calls_producer import update

def test_producer():
  event = {
    "Records": [
      {
        "Sns": {
          "Message": "{\"account\": \"cardinal\",\"account_id\": 8}"
        }
      }
    ]
  }
  assert update(event,{}) == None

