import sys
import os
import traceback
import psycopg2
import boto3
import json

from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.customErrors import report_to_rollbar, ProPairError

env = os.environ["ENV"]
client = boto3.client('ssm')

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]), 
	WithDecryption=True
)  

db_host = os.environ["REDSHIFT_ENDPOINT"]	# PRODUCTION
db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_pass = response['Parameter']['Value']
db_port = os.environ["REDSHIFT_DB_PORT"]

def update(event, context):
	try:
		conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()
		sns = boto3.client('sns')

		print("::::: Fetching Account Info")
		sq = "SELECT id, name, velocify_username, velocify_password FROM accounts;"

		cur.execute(sq)
		data = cur.fetchall()

		is_twelve_hour = True if 'format' in event else False 
		print("::::: Scheduler Format: {}".format("12 hours" if is_twelve_hour else "5 minutes"))

		for account_row in data:
			
			message = {
				'account_id': account_row[0],
				'account': account_row[1],
				'velocify_username': account_row[2],
				'velocify_password': account_row[3]
			}

			account_config = get_account_config(message['account_id'], cache=False)

			try:
				triggers = account_config['leads_producer']['triggers']
			except KeyError:
				err = "Unable to find configuration in DynamoDB: <leads_producer.triggers>"
				rollbar_error = ProPairError(err, error_type="LeadsScheduler")
				rollbar_error.account = message['account']
				secure_log(rollbar_error)
				report_to_rollbar(os.environ["TOPIC_ROLLBAR"], rollbar_error)
				continue

			

			run_incremental = triggers['run_producer'] if 'run_producer' in triggers else False
			scheduler_format = triggers['scheduler_format'] if 'scheduler_format' in triggers else '5 minutes'
			
			if (run_incremental):
				if ((is_twelve_hour and scheduler_format == "12 hours") or (not is_twelve_hour and scheduler_format == '5 minutes')):
					secure_log("::::: Running redshift {} incremental for account {}".format(scheduler_format, message['account']))
					response = sns.publish(
						TopicArn=os.environ["TOPIC_DATA_ARCH_LEADS_REDSHIFT"], 
						Subject="Production Error in Recommend Lambda",	 
						Message=json.dumps(message)
					)

					secure_log(response)
				else:
					secure_log("::::: Mismatched scheduler. Expected Format: {}".format(scheduler_format))
			else:
				secure_log("::::: Leads Incremental set to false for account {}. Skipping...".format(message['account']))
			
		conn.commit()
		cur.close()
		conn.close()
	except Exception as err:
		secure_log("::::: ERROR :::::")
		secure_log(err)
		
		stack = traceback.format_exc() 
		rollbar_error = ProPairError(err, error_type="LeadsScheduler", exec_info=sys.exc_info(), stack=stack)
		report_to_rollbar(os.environ["TOPIC_ROLLBAR"], rollbar_error)
		conn.rollback()
