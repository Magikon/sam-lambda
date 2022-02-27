import sys
import os
import traceback
import psycopg2
import boto3
import json

from decimal import Decimal
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.customErrors import report_to_rollbar, ProPairError

db_name = os.environ["REDSHIFT_DB_NAME"]
db_host = os.environ["REDSHIFT_ENDPOINT"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = os.environ["REDSHIFT_DB_PORT"]
env = os.environ["ENV"]

#### Getting DB credentials

client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 

db_pass = response['Parameter']['Value']

def decimal_default(obj):
	if isinstance(obj, Decimal):
		return float(obj)
	raise TypeError

def update(event, context):
	try:
		conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()

		sns = boto3.client('sns')

		print("::::: Fetching Account Info")
		sq = "SELECT id, name, velocify_username, velocify_password FROM accounts WHERE velocify_username IS NOT NULL AND velocify_password IS NOT NULL;"

		cur.execute(sq)
		data = cur.fetchall()
		for account_row in data:
			message = {
				'account_id': account_row[0],
				'account': account_row[1],
				'velocify_username': account_row[2],
				'velocify_password': account_row[3]
			}
			account_config = get_account_config(message['account_id'], cache=False)
			message['account_config'] = account_config

			try:
				triggers = account_config['events_producer']['triggers']
			except KeyError:
				err = "Unable to find configuration in DynamoDB: <events_producer.triggers>"
				rollbar_error = ProPairError(err, error_type="EventsScheduler")
				rollbar_error.account = message['account']
				secure_log(rollbar_error)
				report_to_rollbar(os.environ["TOPIC_ROLLBAR"], rollbar_error)

				continue
			
			run_incremental = triggers['run_producer'] if 'run_producer' in triggers else False
			
			if (run_incremental):
				secure_log("::::: Running redshift incremental for account {}".format(message['account']))
				response = sns.publish(
					TopicArn=os.environ["TOPIC_DATA_ARCH_EVENTS"], 
					Subject="Events Incremental",   
					Message=json.dumps(message, default=decimal_default)
				)
				secure_log(response)
			else:
				secure_log("::::: Events Incremental set to false for account {}. Skipping...".format(message['account']))
			
		conn.commit()
		cur.close()
		conn.close()
	except Exception as err:
		secure_log("::::: ERROR :::::")
		secure_log(err)
		
		stack = traceback.format_exc()
		rollbar_error = ProPairError(err, error_type="EventsScheduler", exec_info=sys.exc_info(), stack=stack)
		report_to_rollbar(os.environ["TOPIC_ROLLBAR"], rollbar_error)
		conn.rollback()
