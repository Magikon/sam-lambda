import sys
import os
import traceback

sys.path.insert(0, './lib/idna')
sys.path.insert(0, './lib/python-certifi')
sys.path.insert(0, './lib/chardet')
sys.path.insert(0, './lib/urllib3')
sys.path.insert(0, './lib/requests')
sys.path.insert(0, './lib')

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
import csv
import xmltodict
import random

from event_types import event_types 
from event_types import action_log_subtypes
from event_types import action_seq
from pplibs.logUtils import secure_log


#### Temporarily hardcoded account metadata:
velocify_get_milestones_url	 = "https://service.leads360.com/ClientService.asmx/GetMilestones"
velocify_get_leads_url  = "https://service.leads360.com/ClientService.asmx/GetLeads"
velocify_get_lead_ids_span_url  = "https://service.leads360.com/ClientService.asmx/GetLeadIdsSpan"

db_name = os.environ["DB_NAME"]
db_user = os.environ["DB_USER"]

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="{}-database-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 

db_pass = response['Parameter']['Value']
db_host = os.environ["DB_ENDPOINT"]

def update(event, context):

	try:
		account_id = event['account_id']
		start_date   = datetime.datetime.strptime(event['start_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M:%S %p") if 'start_date' in event else None
		end_date   = datetime.datetime.strptime(event['end_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M:%S %p") if 'end_date' in event else None
		minutes_from = event['fromNowMinutes'] if 'fromNowMinutes' in event else None

		secure_log("::::: Fetching account")

		conn = psycopg2.connect('host={} dbname={} user={} password={}'.format(db_host, db_name, db_user, db_pass))
		cur = conn.cursor()
		sq = "SELECT * FROM accounts WHERE id={} AND velocify_username IS NOT NULL AND velocify_password IS NOT NULL".format(account_id)
		cur.execute(sq)
		response = cur.fetchone()
		
		if response != None:
			account = { 'id': response[0], 'name': response[1].upper(), 'velocify_username': response[3], 'velocify_password': response[4] }

			secure_log("::::: Fetching Customer Milestones for {}".format(account['name']))
			
			payload = {'username':account['velocify_username'], 'password': account['velocify_password']}
			r = requests.post(velocify_get_milestones_url, data = payload)
			
			secure_log("------------>")
			secure_log(r.text)
			data = xmltodict.parse(r.text, attr_prefix='', dict_constructor=dict)
			milestones = []
			for m in data['Milestones']['Milestone']:
				milestones.append(m)
			
			secure_log("::::: Fetching {} Leads from {} to {}".format(account['name'], start_date, end_date))
			
			if (minutes_from):
				payload = {'username':account['velocify_username'], 'password': account['velocify_password'], 'fromNowMinutes': minutes_from}
				r = requests.post(velocify_get_lead_ids_span_url, data = payload)
			else:
				payload = {'username':account['velocify_username'], 'password': account['velocify_password'], 'from': start_date, 'to': end_date}
				r = requests.post(velocify_get_leads_url, data = payload)

			# defining a params dict for the parameters to be sent to the API
			
			# sending get request and saving the response as response object
			
			# extracting data in XML format
			events = []
			leads = xmltodict.parse(r.text, attr_prefix='', dict_constructor=dict)

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

			def read_leads(lead):
				lead_id = lead['Id']
				lead_modified_at = lead['ModifyDate']

				for item in lead.items():
					lead_attribute = item[0]
					lead_value = item[1]

					if(lead_attribute == 'Logs'):
						try:
							for log_type in lead_value.items():
								name = log_type[0]
								value = log_type[1] 
								events.extend(parse_logs(value, name, lead_id, lead_modified_at))
						except Exception as e:
							secure_log(':: ERROR :: {}'.format(e))
							Nerror += 1
					
					

			Nerror=0
			action_count=0
			if leads['Leads'] != None and 'Lead' in leads['Leads']:
				secure_log("::::: TOTAL LEADS PULLED: {} FOR {}".format(len(leads['Leads']['Lead']), account['name']))
				if (isinstance(leads['Leads']['Lead'], list)):
					for lead in leads['Leads']['Lead']:

						read_leads(lead)
						#Randomly assign action logs for staging
						if (os.environ["ENV"] == "staging" and bool(random.choice([False, False, True, False, False]))):
							max_id = -int(events[len(events)-1]['LogId'])
							incremental_date = datetime.datetime.strptime(lead['CreateDate'], '%m/%d/%Y %H:%M:%S')
							for subtype in random.choice(action_seq):
								action_count = action_count + 1
								max_id = max_id - 1
								rand_h = random.randint(0,2)
								rand_m = random.randint(1,60)
								incremental_date = incremental_date + datetime.timedelta(hours=rand_h) + datetime.timedelta(minutes=rand_m)
								if (incremental_date.hour < 8):
									incremental_date = incremental_date + datetime.timedelta(hours=(8 - incremental_date.hour))
								action_event = {
									"Id": lead['Id'],
									"LogId": max_id,
									"ActionDate": incremental_date.strftime('%m/%d/%Y %H:%M:%S'),
									"ActionTypeId": subtype,
									"ActionTypeName": random.choice(action_log_subtypes[subtype])['log_subtype_name'],
									"Message": "",
									"LogType": "Action",
									"AgentName": lead['Agent']['AgentName'],
									"AgentId": lead['Agent']['AgentId'],
									"AgentEmail": lead['Agent']['AgentEmail'],
									"GroupId": "",
									"GroupName": "",
									"ModifiedAt": lead['ModifyDate'],
									"MilestoneId": random.choice(action_log_subtypes[subtype])['milestone_id'],
								}
								events.append(action_event)
				else:
					read_leads(leads['Leads']['Lead'])
			else:
				secure_log("No leads found from {} to {}".format(start_date, end_date))
				return
			
			if(os.environ["ENV"] == "staging"):
				secure_log("::::: Randomly Generated Action Events: {}".format(action_count))

			normalized_events = []
			for event in events:
				normalized_event = { 'account_id': account['id'] }
				# secure_log(event['LogType'].lower())
				# print event_types[event['LogType'].lower()]
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

			secure_log("::::: TOTAL LEAD ERRORS: {} FOR {}".format(Nerror, account['name']))
			secure_log("::::: TOTAL EVENTS TO PROCESS: {} FOR {}".format(len(normalized_events), account['name']))

			#upload_to_s3(account['name'], normalized_events)

			csv = insert_to_csv(normalized_events, cur, conn, account, start_date, end_date)
			
			upload_to_s3(account, csv)

			cur.close()
			conn.close()
		else:
			secure_log("::::: ERROR! ACCOUNT ID {} NOT FOUND".format(account_id))
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')

		

def upload_to_s3(account, csv_file):
	secure_log("::::: Uploading CSV to S3")

	s3 = boto3.resource('s3')
	s3.meta.client.upload_file(csv_file, os.environ["BUCKET"], 'events-tmp2/{}/{}'.format(account['name'].lower(),csv_file))

	secure_log("::::: Done")

def insert_to_csv(records, cur, conn, account, start_date, end_date):
	secure_log("::::: Saving to DB")	
	try:
		if len(records) > 0:
			secure_log("::::: INSERTING ROWS")
			insert = get_insert_statement(records, cur, conn)
			start_csv_date = datetime.datetime.strptime(start_date, "%m/%d/%Y %I:%M:%S %p").strftime("%Y-%m-%d_%H:%M:%S")
			end_csv_date = datetime.datetime.strptime(end_date, "%m/%d/%Y %I:%M:%S %p").strftime("%Y-%m-%d_%H:%M:%S")

			csv_file = "{}_{}_to_{}.csv".format(account['name'], start_csv_date, end_csv_date)
			try:
				with open(csv_file, 'w') as csvfile:
					writer = csv.DictWriter(
					csvfile, 
					fieldnames=insert['csv_columns'],
					delimiter="|"
					)
					writer.writeheader()
					for data in insert['events']:
						if (data['log_note'] is not None):
							data['log_note'] = re.sub("\\n|\\r", "", data['log_note'])
						writer.writerow(data)
			except IOError as err:
				secure_log("I/O error: ", err)

			secure_log("::::: TOTAL EVENTS INSERTED: {} FOR {}".format(len(insert['events']), account['name']))
			secure_log("::::: CSV LOCATION: {}".format(csv_file))
			return csv_file
		else:
			secure_log("There isn't anything to insert")

	except Exception as err:
		secure_log("----> DB error: " + str(err))
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
		conn.rollback()

def get_insert_statement(records, cur, conn):
	secure_log("::::: Creating CSV insert statement")  

	query = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = 'external_events'"
	cur.execute(query)
	data = cur.fetchall()

	csv_columns = []
	ignored_columns = ['id']

	log_types = get_log_types(cur, conn)

	for line in data:
		if line[0] not in ignored_columns:
			csv_columns.append(line[0])

	events = []
	id_count = 0
	for i, record in enumerate(records):
		event = dict(record)
		id_count = id_count + 1

		if 'log_type' in event:
			for log in log_types:
				if log[1] == event['log_type']:
					event['log_type_id'] = log[0]
					break

		
		date = datetime.datetime.now()

		for x, key in enumerate(csv_columns):
			if key == 'log_subtype_id':
				value = event[key] if key in event else -50
			elif key == "created_at" or key == "updated_at":
				value = "{}".format(date)
			#elif key == "id":
				#value = id_count
			else:
				value = event[key] if key in event else None

			event[key] = value

		events.append(event)

	return {'events': events, 'csv_columns': csv_columns}


def get_log_types(cur, conn):
	try:
		secure_log("--------------------- Getting log_types from database")
		sq = "SELECT log_type_id, log_type FROM log_type_lookup"
		cur.execute(sq)
		data = cur.fetchall()
		return data
	except Exception as err:
		secure_log("-----> DB error getting the log_types" + str(err))
		secure_log(sq)
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')

def sanitize_and_cast(val, to_data_type):
	if(val == None):
		return None
	elif( "INT" in to_data_type):
		#TODO: Sanitize before casting
		if isinstance(val, str):
			if(str(val).lower() == 'true'):
				return 1
			elif(str(val).lower() == 'false'):
				return 0
			else:
				numvalue = re.sub("[^eE0-9.+-]", "", str(val))
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
		elif (isinstance(val,bool)):
			return int(bool(val))
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
