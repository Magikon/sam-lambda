import psycopg2
from calls_producer import update

event = {
  "account_id": 2,
  "account": "nbkc",
  "start_date": "2020-08-01 00:00:00",
  "end_date": "2020-08-03 00:00:00",
}

update(event, {})

#ENV=production REDSHIFT_DB_NAME=production REDSHIFT_DB_USER=rsuser REDSHIFT_ENDPOINT=propair-rs.cbwhpsk1qhvp.us-west-1.redshift.amazonaws.com REDSHIFT_DB_PORT=5439 python3 test_calls.py
