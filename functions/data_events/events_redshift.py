import sys
import os
import traceback

import urllib3 as urllib3
import urllib
import requests
import xml.etree.ElementTree as ET
from pprint import pprint
import psycopg2
import string
import datetime
import re
import boto3
import json
import time
import xmltodict
from faker import Faker
fake = Faker()

from event_types import event_types 
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log



#### Temporarily hardcoded account metadata:
velocify_get_milestones_url	 = "https://service.leads360.com/ClientService.asmx/GetMilestones"
velocify_get_lead_ids_span_url  = "https://service.leads360.com/ClientService.asmx/GetLeadsSpan"
velocify_get_report_url		 = "https://service.leads360.com/ClientService.asmx/GetReportResultsWithoutFilters"

db_name = os.environ["REDSHIFT_DB_NAME"]
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
db_host = os.environ["REDSHIFT_ENDPOINT"]

def update(event, context):

	try:
		
		account = {
			"id": event['account_id'],
			"name": event['account'],
			"velocify_password": event['velocify_password'],
			"velocify_username": event['velocify_username']
		}
		account_config = event['account_config'] if 'account_config' in event else get_account_config(account['id']) 

		#account_config = get_account_config(account['id']) 
		from_now_minutes = int(account_config['event_data_fromNowMinutes']) if 'event_data_fromNowMinutes' in account_config else 7
		
		secure_log("Initializing Secure Logging", configuration=account_config)

		conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()

		secure_log("::::: Fetching Customer Milestones for {}".format(account['name']))
		
		payload = {'username':account['velocify_username'], 'password': account['velocify_password']}
		r = requests.post(velocify_get_milestones_url, data = payload)
		
		secure_log("------------>")
		#secure_log(r.text)
		data = ET.fromstring(r.text)
		milestones = []
		for m in data:
			milestones.append(m.attrib)

		secure_log("::::: Fetching List of Recently Changed Leads for {}".format(account['name']))
		
		# defining a params dict for the parameters to be sent to the API
		secure_log('fromNowMinutes = {}'.format(from_now_minutes))
		payload = {'username':account['velocify_username'], 'password': account['velocify_password'], 'fromNowMinutes': from_now_minutes}
		
		# try sending get request and saving the response as response object
		try:
			r = requests.post(velocify_get_lead_ids_span_url, data = payload)
			leads = xmltodict.parse(r.text, attr_prefix='', dict_constructor=dict)
		except Exception as e:
			try:
				secure_log('RETRY 1 . requests.post failure . {}'.format(e))
				print(r.text)
				r = requests.post(velocify_get_lead_ids_span_url, data = payload)
				leads = xmltodict.parse(r.text, attr_prefix='', dict_constructor=dict)
			except:
				try:
					secure_log('RETRY 2 . requests.post failure . {}'.format(e))
					r = requests.post(velocify_get_lead_ids_span_url, data = payload)
					leads = xmltodict.parse(r.text, attr_prefix='', dict_constructor=dict)
				except:
					secure_log('requests.post failure . {}'.format(e))
					r = None
					leads = None
						
		# extracting data in XML format
		events = []
		lead_id = None
		lead_modified_at = None
		
		def parse_logs(entry, log_type, lead_id, lead_modified_at):
			items = []
			
			if isinstance(entry, dict):
				if (len(entry.keys()) < 2 or 'LogId' not in entry):	
					for item in entry.items():
						items.extend(parse_logs(item[1], item[0], lead_id, lead_modified_at))
				else: 
					event = entry
					event['Id'] = lead_id
					event['LogType'] = log_type
					event['ModifiedAt'] = lead_modified_at
					items.append(event)
			elif isinstance(entry, list):
				for item in entry:
					event = item
					event['Id'] = lead_id
					event['LogType'] = log_type
					event['ModifiedAt'] = lead_modified_at
					items.append(event)
			
			return items

		if leads != None and leads['Leads'] != None and 'Lead' in leads['Leads']:
			Nlead = 0
			for lead in leads['Leads']['Lead']:
				lead_id = lead['Id']
				lead_modified_at = lead['ModifyDate']
				Nlead += 1
				for item in lead.items():
					lead_attribute = item[0]
					lead_value = item[1]

					if(lead_attribute == 'Logs'):
						for log_type in lead_value.items():
							name = log_type[0]
							value = log_type[1] 
							events.extend(parse_logs(value, name, lead_id, lead_modified_at))
		else:
			raise Exception("TOTAL NEW/UPDATED LEADS = 0 . TOTAL EVENTS = 0")			

		normalized_events = []
		for event in events:
			normalized_event = { 'account_id': account['id'] }

			for attribute, propair_field in event_types[event['LogType'].lower()].items():
				if attribute in event: 
					normalized_event[ propair_field['field_name' ] ] = sanitize_and_cast( event[ attribute ], propair_field['field_type'] )
			
			normalized_event['account_lead_id']	 = sanitize_and_cast( event['Id'], "INT" )
			normalized_event['log_type']			= sanitize_and_cast( event['LogType'], "VARCHAR" )
			normalized_event['modified_at']		 = sanitize_and_cast( event['ModifiedAt'], "VARCHAR" )
			#normalized_event['created_at']		  = str(datetime.datetime.now())

			if('milestone_id' in normalized_event):
				try:
					milestone = next((item for item in milestones if item["MilestoneId"] == str(normalized_event['milestone_id'])), None)
					milestone_name = milestone['MilestoneTitle']
					normalized_event['milestone_name'] = sanitize_and_cast( milestone_name, "VARCHAR" )
				except:
					normalized_event['milestone_name'] = sanitize_and_cast( "NULL", "VARCHAR" )

			normalized_events.append( normalized_event )


		secure_log("TOTAL NEW/UPDATED LEADS = {} . TOTAL EVENTS = {}".format(Nlead,len(normalized_events)) )
		#upload_to_s3(account['name'], normalized_events)
		stored_procedure = "sp_external_events_increment"

		def split_into_chunks(df, size):
			li = []
			i = 0
			while i < len(df):
				secure_log("[{}:{}]".format(i, i+size))
				li.append(df[i:i+size])
				i += size
			return li

		chunk_limit = account_config['events_chunk_limit'] if 'events_chunk_limit' in account_config else 10000

		if (len(normalized_events) > chunk_limit):
			secure_log("::::: Events Length Above {}! Splitting into chunks...".format(chunk_limit))
			events_chunks = split_into_chunks(normalized_events, chunk_limit)
			secure_log("::::: {} chunks to insert".format(len(events_chunks)))
			for i, data in enumerate(events_chunks):
				insert_to_db(data, i, cur, conn, stored_procedure)
		else:
			insert_to_db(normalized_events, None, cur, conn, stored_procedure)
		
		cur.close()
		conn.close()
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')

		

def upload_to_s3(account, record):
	secure_log("::::: Uploading to S3")
	client = boto3.client('s3')
	body = json.dumps(record)[1:-1].replace('}, {', '}\n{')
	client.put_object(Body=body, Bucket=os.environ['BUCKET'], Key="logs/external_events/dt={}/external_events_{}_{}.txt".format(datetime.date.today(), account, time.time()))


def insert_to_db(records, chunk_num, cur, conn, stored_procedure):
	try:
		if len(records) > 0:
			chunk = " Chunk: {}".format(chunk_num) if chunk_num != None else ""
			transaction_id = fake.random_int()
			secure_log("::::: [{}]{}. Upserting {} records to DB".format(transaction_id, chunk, len(records)))	

			## - Upsert Procedure:
			# BEGIN
			#1 - Lock events table to prevent serializable errors
			#2 - Create temporary table to store event data
			#3 - Insert event data into temporary table
			#4 - Update external_events with matched unique events from temporary table
			#5 - Deleted matched unique events from temporary table in order to only leave 'new' events
			#6 - Insert remaining 'new' events from temporary table into external_events
			# END   

			#Set this to the temporary table name
			stage_table_name = "stage"
			
			sq = 'begin read write;\
				lock external_events;\
				create temp table {0} (like external_events);\
				alter table {0} drop column id;'.format(stage_table_name)

			#This will retrieve data insert, update, delete and final insert query
			sq += get_insert_statement(records, stage_table_name, cur, conn)

			sq += "end transaction;\
			drop table stage;"
			count = 0
			successful = False
			while not successful and count < 5:
				try:
					cur.execute(sq)
					conn.commit()
					successful = True
				except Exception as e:
					secure_log("::::: [{}]{} Error - {} trying again...".format(transaction_id, chunk, e))
					secure_log("::::: [{}]{} Num of retries: {}".format(transaction_id, chunk, count))
					cur.execute('rollback;')
					conn.commit()
					time.sleep(0.5*count)

				count += 1
			
			if (successful):
				secure_log("::::: [{}]{} Rows upserted".format(transaction_id, chunk))
			else:
				raise Exception("[{}]{} After {} retries upsert was unable to complete".format(transaction_id, chunk, count))
		else:
			secure_log("There isn't anything to insert")

	except Exception as err:
		secure_log("----> DB error: " + str(err))
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
		conn.rollback()

def get_insert_statement(records, stage_name, cur, conn):

	secure_log("Find data types")
	sq = "SELECT \"column\",type from pg_table_def where tablename='external_events'"
	cur.execute(sq)

	column_data = cur.fetchall()
	columns = {}
	ignored_columns = ['id']

	#Set this to the unique columns required for this table
	unique_columns = ['account_id', 'account_lead_id', 'log_id', 'log_subtype_id']

	for line in column_data:
		if line[0] not in ignored_columns:
			columns[line[0]] = {"name": line[0], "type": line[1]}
	
	log_types = get_log_types(cur, conn)

	#Query start
	query       = 'INSERT INTO stage({})'.format(', '.join(columns.keys()))
	#This variable will store the insert query for the event data
	values      = "VALUES "
	#This variable will store the update query for existing events
	update_query = "update external_events set "
	#This variable will store the delete query removing existing events
	delete_query = "delete from {} using external_events where ".format(stage_name)
	#This variable will store the insert query for remaining non-existant events
	insert_query = "insert into external_events"

	update_values = []
	insert_values = []

	for col in columns:
		insert_values.append(col)
		if (col not in unique_columns and col != 'created_at'):
			update_values.append("{0} = {1}.{0}".format(col, stage_name))

	insert_query += "({0}) select {0} from {1};".format(', '.join(insert_values), stage_name)		
	update_query += "{}".format(', '.join(update_values))
	update_query += " from {} where ".format(stage_name)

	for i, uniq in enumerate(unique_columns):
		update_query += "external_events.{0} = {1}.{0}".format(uniq, stage_name)
		delete_query += "external_events.{0} = {1}.{0}".format(uniq, stage_name)
		if(i < len(unique_columns)-1):
			update_query += " and "
			delete_query += " and "
		else:
			update_query += ";"
			delete_query += ";"

	for i, record in enumerate(records):

		if 'log_type' in record:
			for log in log_types:
				if log[1] == record['log_type']:
					record['log_type_id'] = log[0]
					break
		
		record_values = []
		date = datetime.datetime.now()

		for x, key in enumerate(columns):
			value = record[key] if key in record else None
			data_type = columns[key]['type']

			if(isinstance(value, str)):
				value = "$${}$$::{}".format(value, data_type)
			elif(key in ['updated_at', 'created_at']):
				value = 'getdate()'
			elif value == None:
				if key == 'log_subtype_id':
					value = '-50'
				else:
					value = "NULL" 
			else:
				value = "{}::{}".format(value, data_type)

			record_values.append(value)

		values += "({})".format(', '.join(record_values))
		if(i < len(records)-1):
			values += ", "

	query  += values + '; ' + update_query + " " + delete_query + " " + insert_query

	return query

def get_log_types(cur, conn):
	try:
		secure_log("--------------------- Getting log_types from database")
		sq = "SELECT log_type_id, log_type FROM log_type_lookup"
		cur.execute(sq)
		data = cur.fetchall()
		return data
	except Exception as err:
		secure_log("-----> DB error getting the log_types", str(err))
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')

def sanitize_and_cast(val, to_data_type):
	if(val == None):
		return None
	if( "CHAR" in to_data_type ):
		#TODO: Sanitize before casting
		return str(val).replace("'", r"''")
	elif( "INT" in to_data_type):
		#TODO: Sanitize before casting
		if(isinstance(val, str)):
			if(val == 'True'):
				return 1
			elif(val == 'False'):
				return 0
			else:
				numvalue = re.sub("[^eE0-9.+-]", "", val)
				numvalue = re.sub("(?<!^)[+-]", "", numvalue)
				if(numvalue):
					try:
						return int( float( numvalue ) )
					except Exception as e:
						# print e
						# print val
						return -1
				else:
					return -1
		else:
			return val
	else:
		return val


def report_to_rollbar(err, stack, exc_info, account):
	secure_log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")

	secure_log(str(err))
	secure_log(stack)
	secure_log(exc_info)

	sns = boto3.client('sns')
	
	py_err = { 'name': str(err), 'message': "EventsDataArchException", 'type': "error", 'trace': {
		'exc_info': "\"{}\"".format( str(exc_info.replace("\"", "'") ) ), 
		'stack': "\"{}\"".format(str(stack).replace("\"", "'"))
		} 
	}
	
	rollbar_error = { 'error': py_err, 'referer': 'Events', 'account': account }
	rollbar_error = json.dumps(rollbar_error)

	response = sns.publish(
		TopicArn=os.environ["TOPIC_ROLLBAR"], 
		Subject="Production Error in Events Lambda",   
		Message=rollbar_error
	)
	secure_log("Response: {}".format(response))

