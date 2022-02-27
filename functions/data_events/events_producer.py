import sys
import os
import traceback

import requests
import xml.etree.ElementTree as ET
import datetime
import re
import boto3
import time
import xmltodict
from time import sleep
import json
from json import dumps
from kafka import KafkaProducer
import yaml 

from event_types import event_types 
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.customErrors import ProPairError, report_to_rollbar

config = yaml.load( open('./config/config.yml'))
config = config[os.environ['ENV']]
host= config["host"]
env = os.environ["ENV"]

#### Getting Kafka credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/kafka-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 
k_pass = response['Parameter']['Value']

#### Temporarily hardcoded account metadata:
velocify_get_milestones_url	 = "https://service.leads360.com/ClientService.asmx/GetMilestones"
velocify_get_lead_ids_span_url  = "https://service.leads360.com/ClientService.asmx/GetLeadsSpan"
velocify_get_report_url		 = "https://service.leads360.com/ClientService.asmx/GetReportResultsWithoutFilters"

rollbar_topic = os.environ["TOPIC_ROLLBAR"]

def on_send_success(record_metadata):
	pass
	#secure_log('{0} {1}  {2}'.format(record_metadata.topic,record_metadata.partition, record_metadata.offset) )

def on_send_error(excp):
	log.error('Something went wrong', exc_info=excp)
	py_err = None
	try:
		if isinstance(excp, str):
			py_err = ProPairError(excp, "Events_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		else:
			py_err = ProPairError(excp.message, "Events_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())

	except Exception as e:
		secure_log(e)
		py_err = ProPairError(str(excp), "Events_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
	py_err.account = ACCOUNT if ACCOUNT is not None and ACCOUNT != "" else "NO_ACCOUNT"
	report_to_rollbar(rollbar_topic, py_err)
	secure_log( "I am unable to connect to the database")
	
def update(event, context):

	try:
		account_meta = json.loads(event['Records'][0]['Sns']['Message'])
		account = {
			"id": account_meta['account_id'],
			"name": account_meta['account'],
			"velocify_password": account_meta['velocify_password'],
			"velocify_username": account_meta['velocify_username']
		}

		account_config = account_meta['account_config'] if 'account_config' in account_meta else get_account_config(account['id']) 
		secure_log("::::: Initialize account encryption", configuration=account_config)
		secure_log("::::: Event", account_meta)
		
		try:
			config_variables = account_config['events_producer']['variables']
		except KeyError:
			raise Exception("Unable to find producer configuration in DynamoDB: <events_producer.variables>")
		
		from_now_minutes = int(config_variables['fromNowMinutes']) if 'fromNowMinutes' in config_variables else 7

		secure_log("::::: Fetching Customer Milestones for {}".format(account['name']))
		
		payload = {'username':account['velocify_username'], 'password': account['velocify_password']}
		r = requests.post(velocify_get_milestones_url, data = payload)
		
		secure_log("------------>")
		#secure_log(r.text)
		try_number = 0
		while try_number < 3:
			try:
				data = ET.fromstring(r.text)
				break
			except Exception as err:
				secure_log(err)
				try_number += 1
				if try_number >= 3:
					raise err
				secure_log("Trying again...")
		
		
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
				secure_log(r.text)
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

		def read_leads(lead):
			nonlocal Nerror
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

		if leads != None and leads['Leads'] != None and 'Lead' in leads['Leads']:
			Nlead = 0
			Nerror = 0
			if (isinstance(leads['Leads']['Lead'], list)):
				for lead in leads['Leads']['Lead']:
					Nlead += 1
					read_leads(lead)
			else:
				Nlead += 1
				read_leads(leads['Leads']['Lead'])
		else:
			secure_log("TOTAL NEW/UPDATED LEADS = 0 . TOTAL EVENTS = 0")	
			return		

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
		
		producer = KafkaProducer(bootstrap_servers=[host],
						 acks= config["acks"],
						 retries= config["retries"],
						 security_protocol='SASL_PLAINTEXT',
						 sasl_mechanism='PLAIN',
						 sasl_plain_username=config["user"],
						 sasl_plain_password=k_pass,
						 linger_ms= config['linger_ms'],
						 batch_size= 32*1024 ,
						 value_serializer=lambda x: dumps(x).encode('utf-8')
						 )

		secure_log("::: Producer created :::")

		# send 1K messages
		for i, e in enumerate(normalized_events):
			#k = '{}{}{}{}'.format(e['account_id'], e['account_lead_id'], e['log_id'], e['log_subtype_id'])
			k = bytes(int(e['account_id']))
			p = producer.send('external_events', key=k, value=e).\
				add_callback(on_send_success).\
				add_errback(on_send_error)
		
		secure_log(":::: {} Records sent to stream".format(len(normalized_events)))
		# block until all sync messages are sent
		producer.flush()
		producer.close()

	except Exception as err:
		py_err = ProPairError(err, "Events_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		secure_log(err)
		try:
			account_meta = json.loads(event['Records'][0]['Sns']['Message'])
			account = account_meta['account']
			py_err.account = account
			report_to_rollbar(rollbar_topic, py_err)
		except Exception as err2:
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)
		

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
						# secure_log e
						# secure_log val
						return -1
				else:
					return -1
		else:
			return val
	else:
		return val

