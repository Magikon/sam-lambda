import sys
import os
import traceback
import random
import requests
import xml.etree.ElementTree as ET
import psycopg2
import datetime
import re
import boto3
import pytz
import xmltodict
import yaml 
import json
import pandas as pd
import gzip

from io import BytesIO, TextIOWrapper
from time import sleep
from json import dumps
from kafka import KafkaProducer
from ricochet import Ricochet

from db_utils import get_unique_agents
from utils import get_unique_agents_ricochet, get_column_data
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.customErrors import ProPairError, report_to_rollbar
from pplibs.kafka_avro import KafkaAvroProducer

sns = boto3.client('sns')

rollbar_topic = os.environ["TOPIC_ROLLBAR"]

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 
db_pass = response['Parameter']['Value']

#### Getting Kafka credentials
response = client.get_parameter(
	Name="/{}/kafka-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 
k_pass = response['Parameter']['Value']

##### Redshift env variables 
db_host = os.environ["REDSHIFT_ENDPOINT"]
db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = 5439

config = yaml.load( open('./config/config.yml'))
config = config[os.environ['ENV']]
host= config["host"]
external_leads_topic='external_leads'
external_lead_details_topic='external_lead_details'

velocify_get_agents_url	= "https://service.leads360.com/ClientService.asmx/GetAgents"
velocify_get_reports_url	= "https://service.leads360.com/ClientService.asmx/GetReports"
velocify_report_url	 = "https://service.leads360.com/ClientService.asmx?op=GetReportResults"

def getAgentMapping(cur, account, account_id):
	secure_log("::::: Fetching Agent Profile Info for account {}".format(account))
	agent_mapping = {}
	try:
		query = 'SELECT id as propair_id, agent_id, name_velocify from agent_profiles WHERE account_id={};'.format(account_id)
		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			agent_mapping[ line[2] ] = { 'propair_id': line[0], 'agent_id': line[1] }
	except Exception as err:
		py_err = ProPairError(err, error_type="Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		py_err.account = account if account is not None and account != "" else "NO_ACCOUNT"
		report_to_rollbar(rollbar_topic, py_err)
		secure_log( "I am unable to connect to the database")

	return agent_mapping

def getCampaignMapping(cur, account, account_id):
	secure_log("::::: Fetching Campaign Info for account {}".format(account))
	campaign_mapping = []
	
	try:
		query = 'SELECT source, source_detail, campaign_group, source_channel, ialgo_source from source_detail_lookup WHERE account_id={};'.format(account_id)
		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			campaign_mapping.append( { 'source': line[0], 'source_detail': line[1], 'campaign_group': line[2], 'source_channel': line[3], 'ialgo_source': line[4] } )
	except Exception as err:
		py_err = ProPairError(err, error_type="Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		py_err.account = account if account is not None and account != "" else "NO_ACCOUNT"
		report_to_rollbar(rollbar_topic, py_err)
		secure_log( "I am unable to connect to the database")

	return campaign_mapping

def on_send_success(record_metadata):
	pass
	#secure_log('{0} {1}  {2}'.format(record_metadata.topic,record_metadata.partition, record_metadata.offset) )

def on_send_error(excp):
	secure_log('Something went wrong', exc_info=excp)
	py_err = None
	try:
		if isinstance(excp, str):
			py_err = ProPairError(excp, "Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		else:
			py_err = ProPairError(excp.message, "Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())

	except Exception as e:
		secure_log(e)
		py_err = ProPairError(str(excp), "Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
	py_err.account = ACCOUNT if ACCOUNT is not None and ACCOUNT != "" else "NO_ACCOUNT"
	report_to_rollbar(rollbar_topic, py_err)
	secure_log( "I am unable to connect to the database")

def query_agents(account_meta):
	# defining a params dict for the parameters to be sent to the API
	payload = {'username': account_meta['velocify_username'], 'password': account_meta['velocify_password']}

	# sending get request and saving the response as response object
	r = requests.post(velocify_get_agents_url, data = payload)
	
	data = None
	
	try:
		data = xmltodict.parse(r.content, attr_prefix='', dict_constructor=dict)
	except Exception as e:
		secure_log('xmltodict failure . {}'.format(e))
		raise Exception('Unable to parse report list; Response: {}'.format(r.content))
	
	agents = {int(x['AgentId']): x for x in data['Agents']['Agent']}
	secure_log("::::: Retrieved {} Agents from Velocify".format(len(agents)))
	return agents
	  
def insert_agent_profiles(account_meta, leads, leads_software):

	if (leads_software == 'velocify'):
		v_agents = query_agents(account_meta)	
		agents = get_unique_agents(leads, account_meta, v_agents)
	elif (leads_software == 'ricochet'):
		agents = get_unique_agents_ricochet(leads, account_meta)

	secure_log( "{} Agents retrieved".format(len(agents)))

	if (len(agents) > 0):

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

		secure_log("::: Agent Profiles Producer created :::")

		for i, e in enumerate(agents):
			k = bytes(int(e['account_id']))
			p = producer.send('agent_profiles', key=k, value=e).\
				add_callback(on_send_success).\
				add_errback(on_send_error)
		
		secure_log(":::: {} agent_profile Records sent to stream".format(len(agents)))

		# block until all sync messages are sent
		producer.flush()
		producer.close()


def sanitize_field(field):
	return re.sub("_+x00..", "", field)

def get_leads(account_meta, scheduler_format, is_data_pull=False):

	leads_result = None

	def fetch_report(report_name):
		secure_log("::::: Fetching Report List for account {}".format(account_meta['account']))
		
		# defining a params dict for the parameters to be sent to the API
		payload = {'username': account_meta['velocify_username'], 'password': account_meta['velocify_password']}

		# sending get request and saving the response as response object
		r = requests.post(velocify_get_reports_url, data = payload)
		
		# extracting data in XML format
		secure_log("------------>")
		
		report = None
		data = None
		
		try:
			data = xmltodict.parse(r.content, attr_prefix='', dict_constructor=dict)
		except Exception as e:
			secure_log('xmltodict failure . {}'.format(e))
			raise Exception('Unable to parse report list; Response: {}'.format(r.content))
		
		try:
			if (isinstance(data, dict)):		
				for child in data['Reports']['Report']:
					if(child['ReportTitle'] == report_name):
						report = child
			else:
				raise Exception("Dict Parsing failure")
		except Exception as e:
			raise Exception("Error parsing report {} - {}".format(report_name, e))

		return report

	def fetch_leads(account_meta, report, filters=[]):

		headers = {'Content-Type': 'text/xml; charset=utf-8',
			"Host": "service.leads360.com",
			"Content-Length": "length",
			"SOAPAction": "https://service.leads360.com/GetReportResults"}
		
		filters_xml = ""

		for item in filters:
			filters_xml += """<FilterItem FilterItemId="{0}">{1}</FilterItem>""".format(item['id'], item['value'])

		body = """<?xml version="1.0" encoding="UTF-8"?>
			<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
				<soap:Body>
					<GetReportResults xmlns="https://service.leads360.com">
						<username>{0}</username>
						<password>{1}</password>
						<reportId>{2}</reportId>
						<templateValues>
							<FilterItems>
								{3}
							</FilterItems>   
						</templateValues>
					</GetReportResults>
				</soap:Body>
			</soap:Envelope>""".format(account_meta['velocify_username'], 
				account_meta['velocify_password'],
				report['ReportId'],
				filters_xml)

		try:
			r = requests.post(velocify_report_url  , data=body, headers=headers)
		except Exception as e:
			try:
				secure_log('RETRY 1 . requests.post failure . {}'.format(e))
				r = requests.post(velocify_get_report_url,data=payload)
			except:
				try:
					secure_log('RETRY 2 . requests.post failure . {}'.format(e))
					r = requests.post(velocify_get_report_url,data=payload)
				except:
					secure_log('requests.post failure . {}'.format(e))
					r = None

		try:
			data = xmltodict.parse(r.content, attr_prefix='', dict_constructor=dict)

		except Exception as e:
			try:
				secure_log('xmltodict failure . {}'.format(e))

				XMLdecode = r.content.decode('utf-8')
				XMLcorrected = XMLdecode.replace('&','')
				data = xmltodict.parse(XMLcorrected)

				secure_log('xmltodict success . {} . ERROR CORRECTED'.format(e))
			except Exception as e:
				secure_log('2nd xmltodict failure . {}'.format(e))
				data = {}

		return data

	if(is_data_pull is True):
		start_date = datetime.datetime.strptime(account_meta['start_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M %p")
		end_date = datetime.datetime.strptime(account_meta['end_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M %p")

		secure_log("::::: Triggered Data Pull. Fetching all leads from {} to {}".format(start_date, end_date))

		report = fetch_report('ProP Data Lead Initial')
		
		if (report is not None):
			secure_log("::::: Found Report ID for ProP Data Lead Initial: {}".format(report['ReportId']))

			filters = []

			if ('FilterItems' in report):
				if('FilterItem' in report['FilterItems']):
					if (isinstance(report['FilterItems']['FilterItem'], list)):
						for child in report['FilterItems']['FilterItem']:
							if (child['Operator'] in ['GreaterThanOrEqualTo', 'GreaterThan']):
								filters.append({'id': child['FilterItemId'], 'value': start_date})
							elif(child['Operator'] in ['LessThanOrEqualTo', 'LessThan']):
								filters.append({'id': child['FilterItemId'], 'value': end_date})
					else:
						secure_log(":::: Error! Missing filters", report)
						raise Exception("Missing filters")
					
			if (len(filters) < 2):
				secure_log(":::: Error! Missing filter ids", report)
				raise Exception("Missing filter ids")


			secure_log("::::: Fetching Report")
			leads_data = fetch_leads(account_meta, report, filters)

			try:
				leads_result = leads_data['soap:Envelope']['soap:Body']['GetReportResultsResponse']['GetReportResultsResult']
			except Exception as e:
				secure_log(e)
				secure_log(leads_data)
				raise Exception('Unrecognized lead data')
			
	else:
		secure_log("::::: No dates provided. Triggering incremental update.")
		
		report_name = 'ProP Data Update 12hour' if scheduler_format == '12 hours' else 'ProP Data Update'
		report = fetch_report(report_name)
		
		if(report is not None):
			secure_log("::::: Found Report ID for {}: {}".format(report_name, report['ReportId']))
			
			filters = []
			if ('FilterItems' in report):
				if('FilterItem' in report['FilterItems']):
					if (isinstance(report['FilterItems']['FilterItem'], list)):
						for child in report['FilterItems']['FilterItem']:
							if (child['FieldTitle'] == 'CustomerId'):
								filters.append({'id': child['FilterItemId'], 'value': account_meta['account_id']})

					
			if (os.environ['ENV'] == 'staging' and len(filters) < 1):
				secure_log(":::: Error! Missing filter for CustomerId", report)
				raise Exception("Missing CustomerId filter")

			secure_log("::::: Fetching Report")
			leads_data = fetch_leads(account_meta, report, filters)

			try:
				leads_result = leads_data['soap:Envelope']['soap:Body']['GetReportResultsResponse']['GetReportResultsResult']
			except Exception as e:
				secure_log(e)
				secure_log(leads_data)
				raise Exception('Unrecognized lead data')

	if ('Result' not in leads_result['ReportResults']):
		secure_log("::::: No leads found! Exiting..")
		raise Exception("Report returned 0 leads")

	leads = []
	if not isinstance(leads_result['ReportResults']['Result'], list):
		leads_result['ReportResults']['Result'] = [leads_result['ReportResults']['Result']]

	for result in leads_result['ReportResults']['Result']:
		lead = {}
		for lead_child in result:
			lead[ lead_child ] = result[lead_child]
		leads.append( lead )

	return leads


def get_global_attr(cur, account, table_name, include_variable = False):
	secure_log("::::: Fetching Global Attribute Mapping {} for account {}".format(table_name, account))
	attribute_map = []
	include_variable_string = "AND include_variable = 1" if include_variable == True else ""

	try:		
		query = "SELECT propair_field, customer_field_name, customer_original_field_name, customer_split, datatype FROM global_attribute_lookup L INNER JOIN accounts A on L.account_id = A.id WHERE A.name = '{0}' AND (table_name='{1}' OR table_name ='all') {2} AND ( customer_field_name IS NOT NULL OR customer_original_field_name IS NOT NULL OR  customer_split IS NOT NULL );".format(account.lower(), table_name, include_variable_string)
		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			attribute_map.append( { 'propair': line[0], 'customer': line[1], 'original': line[2], 'split': line[3], 'datatype': line[4] } )

		return attribute_map
	except Exception as err:
		py_err = ProPairError(err, "Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		try:
			py_err.account = account
			report_to_rollbar(rollbar_topic, py_err)
		except Exception as err2:
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)

def calculate_credit(score):
	credit_profile = ''
	credit_score = re.sub(r"[^0-9]", "", str(score))
	credit_score_str = re.sub(r"[^a-zA-Z]", "", str(score))
	if credit_score != "":
		if len(credit_score) == 3 or len(credit_score) == 6:
			credit_score = int(credit_score) if int(credit_score) < 1000 else (int(credit_score[0: 3]) + int(credit_score[3: 6])) / 2
			credit_profile = 'Poor' if credit_score > 0 else credit_profile
			credit_profile = 'Fair' if credit_score >= 620 else credit_profile
			credit_profile = 'Good' if credit_score >= 660 else credit_profile
			credit_profile = 'Excellent' if credit_score >= 720 else credit_profile

	
	elif credit_score_str != "":
		credit_score = re.sub(r"[^a-zA-Z]", "", str(score))
		credit_score = credit_score.upper()
		credit_profile = 'Poor' if credit_score == "POOR" else credit_profile
		credit_profile = 'Fair' if credit_score == "FAIR" else credit_profile
		credit_profile = 'Good' if credit_score == "GOOD" else credit_profile
		credit_profile = 'Excellent' if credit_score == "EXCELLENT" else credit_profile
	return credit_profile


def clean_field(lead, priority_value):
	field = lead.get(priority_value, None)
	bin_result = None
	if field is not None:
		try:
			result = calculate_credit(field)
			if result != '':
				bin_result = result
				
		except Exception as err:
			secure_log(err)
			secure_log("TRYING LOOKUP TABLE...")
	return bin_result

def search_in_lookups(lead, value, lookups, with_source=False):
	bin_result = None
	
	credit_profile = value
	if with_source and credit_profile is not None:
		source = lead.get("source", None)
		for i in lookups:
			if i["source"] == source and i["credit_profile_stated"] == credit_profile:
				bin_result = i["bin_credit_profile_stated"]
				break
	elif not with_source and credit_profile is not None:
		for i in lookups:
			if i["credit_profile_stated"] == credit_profile:
				bin_result = i["bin_credit_profile_stated"]
				break
	return bin_result

def get_lookup(account_id, cur):
	result = []
	try:
		query = "SELECT bin_credit_profile_stated, source, credit_profile_stated FROM credit_profile_stated_lookup WHERE account_id={}".format(account_id)
		cur.execute(query)
		data = cur.fetchall()
		for value in data:
			result.append( { 'bin_credit_profile_stated': value[0], 'source': value[1], 'credit_profile_stated': value[2] } )
	except Exception as err:
		secure_log("Error connecting to redshift")
	return result

def get_bin_credit_profile(lead, account_config, lookup):
	result = None
	priority_list = account_config.get("credit_profile_priority_list", None)
	#if priority_list is None:
	priority_list = ["credit_score_stated", "credit_score_range_stated", "credit_profile_stated"]
	for priority_value in priority_list:
		result = clean_field(lead, priority_value)
		index = priority_list.index(priority_value)
		if result is None:
			for i in range(2):
				with_source = True if i == 0 else False
				result = search_in_lookups(lead, lead.get(priority_value, None), lookup, with_source=with_source)
				if result is not None:
					break
		
		if result is None:
			continue
		else:
			break

	return result

def update(event, context):
	try:
		if ('Records' in event):
			account_meta = json.loads(event['Records'][0]['Sns']['Message'])		
		else:
			account_meta = event
		
		account_id		  = account_meta['account_id']
		account_config	  = get_account_config(account_id)

		secure_log("::::: Connecting to database..")
		conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()

		#Get account info if missing
		if any(x not in account_meta for x in ['velocify_username', 'velocify_password', 'account']):
			secure_log("::::: Account data missing! Fetching from DB")
			sq = 'SELECT name,velocify_username, velocify_password from accounts where id = {}'.format(account_id)
			cur.execute(sq)
			account_data = cur.fetchone()
			account_meta['account'] = account_data[0]
			account_meta['velocify_username'] = account_data[1]
			account_meta['velocify_password'] = account_data[2]
			
		account				= account_meta['account']
		velocify_username   = account_meta['velocify_username']
		velocify_password   = account_meta['velocify_password']
		
		try:
			config_variables = account_config['leads_producer']['variables']
		except KeyError:
			raise Exception("Unable to find producer configuration in DynamoDB: <leads_producer.variables>")
		
		try:
			config_triggers = account_config['leads_producer']['triggers']
		except KeyError:
			raise Exception("Unable to find producer configuration in DynamoDB: <leads_producer.triggers>")
		leads_software = config_triggers['leads_software'] if 'leads_software' in config_triggers else 'velocify'
		scheduler_format = config_triggers['scheduler_format'] if 'scheduler_format' in config_triggers else '5 minutes'

		secure_log("::::: SCHEDULER FORMAT: {}".format(scheduler_format))
		secure_log("::::: PRODUCER CONFIG {}".format(config_variables))
		secure_log("::::: ACCOUNT META", account_meta)
		secure_log("::::: LEADS SOFTWARE: ", leads_software)

		report_id = account_meta['report_id'] if 'report_id' in account_meta else None
		external_leads_table = account_meta['external_leads_table'] if 'external_leads_table' in account_meta else 'external_leads'
		external_lead_details_table = account_meta['external_lead_details_table'] if 'external_lead_details_table' in account_meta else 'external_lead_details'

		attribute_map = get_global_attr(cur, account, 'lead')
		detail_attribute_map = get_global_attr(cur, account, 'lead_detail', include_variable=True)
		credit_profile_lookup = get_lookup(account_id, cur)

		l_columns = get_column_data('external_leads', cur)
		ld_columns = get_column_data('external_lead_details', cur)

		is_data_pull = False
	
		if (all(x in account_meta for x in ['start_date', 'end_date'])):
			is_data_pull = True
		
		if (leads_software == 'velocify'):
			leads = get_leads(account_meta, scheduler_format, is_data_pull)
		elif(leads_software == 'ricochet'):
			rcc = Ricochet(conn, {'id': account_id, 'name': account})
			leads,profile_dict = rcc.get_lead_updates()
	
		if(leads != None):

			normalized_leads = []
			normalized_lead_details = []

			secure_log( "*************")
			secure_log( "Leads Retrieved {}".format(len(leads)))
			secure_log( "*************")

			# Make sure we're up-to-date in the agent_profiles table
			try:
				insert_agent_profiles(account_meta, leads, leads_software)
			except Exception as err:
				py_err = ProPairError(err.message if hasattr(err, 'message') else err , "Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
				py_err.account = account if account is not None and account != "" else "NO_ACCOUNT"
				report_to_rollbar(rollbar_topic, py_err)

			agent_mapping = getAgentMapping(cur, account, account_id)
			campaign_mapping = getCampaignMapping(cur, account, account_id)

			secure_log( "Campaign Mapping ------->")
			#print(campaign_mapping)

			secure_log( "::::: Normalizing Leads")
			Nno_firstassign = 0
			Nno_loan_officer = 0
			Nno_user = 0
			Nno_profile_id_user = 0
			missing_users = []
			missing_first_assignment_users = []

			for lead in leads:
				normalized_lead = { 'account_id': account_id }
				normalized_lead_detail = { 'account_id': account_id }

				accepted_fields = config_variables['accepted_split_fields'] if 'accepted_split_fields' in config_variables else []

				#Sanitizing Field Names for Mapping
				new_lead = lead.copy()
				Ncol = len(new_lead.keys())
				for key, value in lead.items():
					if (sanitize_field(key) != key):
						new_lead[sanitize_field(key)] = lead[key]
						del new_lead[key]

				lead = new_lead.copy()

				for attribute in attribute_map:
					datatype = l_columns[attribute['propair']]['type'] if attribute['propair'] in l_columns else attribute['datatype'] 

					#TODO: Handle ':' string splits
					if attribute['original'] in lead:
						normalized_lead[ attribute['propair'] ] = sanitize_and_cast( lead[ attribute['original'] ], datatype, attribute['propair'], accepted_fields )
					elif ':' in attribute['customer']: 
						components = attribute['customer'].split(':')
						if components[0] in lead:
							values = lead[ components[0] ].split(attribute['split'])
							if(len(values) > int(components[1])):
								normalized_lead[ attribute['propair'] ] = sanitize_and_cast( values[ int(components[1]) ], datatype, attribute['propair'], accepted_fields )
					elif attribute['customer'] in lead: 
						normalized_lead[ attribute['propair'] ] = sanitize_and_cast( lead[ attribute['customer'] ], datatype, attribute['propair'], accepted_fields )
				
				#### Cleanup Zip Codes
				if 'property_zip_code_stated' in normalized_lead and normalized_lead['property_zip_code_stated'] is not None and len(normalized_lead['property_zip_code_stated']) > 5:
					normalized_lead['property_zip_code_stated'] = normalized_lead['property_zip_code_stated'][:5]

				normalized_leads.append( normalized_lead )

				for attribute in detail_attribute_map:
					datatype = ld_columns[attribute['propair']]['type'] if attribute['propair'] in ld_columns else attribute['datatype'] 
					#TODO: Handle ':' string splits
					if attribute['original'] in lead:
						normalized_lead_detail[ attribute['propair'] ] = sanitize_and_cast( lead[ attribute['original'] ], datatype, attribute['propair'], accepted_fields )
					elif ':' in attribute['customer']: 
						components = attribute['customer'].split(':')
						if components[0] in lead:
							values = lead[ components[0] ].split(attribute['split'])
							
							if(len(values) > int(components[1])):
								normalized_lead_detail[ attribute['propair'] ] = sanitize_and_cast( values[ int(components[1]) ], datatype, attribute['propair'], accepted_fields )
					elif attribute['customer'] in lead: 
						normalized_lead_detail[ attribute['propair'] ] = sanitize_and_cast( lead[ attribute['customer'] ], datatype, attribute['propair'], accepted_fields )

				#################################
				# Create lookup to profile table
				#################################
				
				if(leads_software == 'velocify'):

					profile_id_loan_officer_default	 = None
					profile_id_first_assignment_user	= None
					profile_id_user					 = None

					if('FirstAssignment_DistributionUser' in lead and lead['FirstAssignment_DistributionUser'] in agent_mapping and 'agent_id' in agent_mapping [ lead['FirstAssignment_DistributionUser'] ]) and agent_mapping [ lead['FirstAssignment_DistributionUser'] ]['agent_id'] != None: 
						profile_id_loan_officer_default  = agent_mapping [ lead['FirstAssignment_DistributionUser'] ]['propair_id']
						profile_id_first_assignment_user = agent_mapping [ lead['FirstAssignment_DistributionUser'] ]['propair_id']
					elif('FirstAssignment_DistributionUser' in lead):
						Nno_loan_officer = Nno_loan_officer+1
						if lead['FirstAssignment_DistributionUser'] not in missing_first_assignment_users:
							missing_first_assignment_users.append(lead['FirstAssignment_DistributionUser'])
					else:
						Nno_firstassign = Nno_firstassign+1 

					if('User' in lead and lead['User'] in agent_mapping and 'agent_id' in agent_mapping [ lead['User'] ]) and agent_mapping [ lead['User'] ]['agent_id'] != None: 
						profile_id_user = agent_mapping [ lead['User'] ]['propair_id']

					elif('User' in lead):
						if lead['User'] not in missing_users:
							missing_users.append(lead['User'])
						Nno_profile_id_user = Nno_profile_id_user+1 
					else:		 
						Nno_user = Nno_user+1 


					normalized_lead_detail['profile_id_loan_officer_default']   = profile_id_loan_officer_default
					normalized_lead_detail['profile_id_first_assignment_user']  = profile_id_first_assignment_user
					normalized_lead_detail['profile_id_user']				   = profile_id_user

				elif (leads_software == 'ricochet'):
					try:
						user = lead['currentowner']
					except Exception as e:
						print("JSON PARSING ERROR: ", e)
						user = None
					
					if (user):
						user_id = int(user['id'])
						user_name = '{}, {}'.format(user['lastName'], user['firstName'])

						if(user_name in agent_mapping and 'agent_id' in agent_mapping[ user_name ]) and agent_mapping [ user_name ]['agent_id'] != None: 
							normalized_lead_detail['profile_id_user'] = agent_mapping [ user_name ]['propair_id']

					################################################################################### 
					# Use profile_group_df to add first_assignment_user, user and loan_officer_default
					################################################################################### 
					if (normalized_lead_detail['account_lead_id'] in profile_dict.keys()):
						normalized_lead_detail['profile_id_user'] = int(profile_dict[normalized_lead_detail['account_lead_id']]['profile_id_user'])
						normalized_lead_detail['profile_id_first_assignment_user'] = int(profile_dict[normalized_lead_detail['account_lead_id']]['profile_id_first_assignment_user'])
						normalized_lead_detail['profile_id_loan_officer_default'] = int(profile_dict[normalized_lead_detail['account_lead_id']]['profile_id_first_assignment_user'])

				normalized_lead_detail['profile_id_qualification']		  = None
				normalized_lead_detail['profile_id_processor']			  = None
				normalized_lead_detail['profile_id_sales_manager']		  = None
				normalized_lead_detail['profile_id_underwriter']			= None
				normalized_lead_detail['profile_id_encompass']			  = None
				
				######################################
				# Add Originated Loans
				######################################

				normalized_lead_detail['second_loan'] = 0
				
				#####################
				# Grab lead domain 
				#####################

				try:
					if 'email_stated' in normalized_lead and normalized_lead['email_stated'] != None:
						email_parts = normalized_lead['email_stated'].split('@')
						if len(email_parts) > 1:
							domain_parts = email_parts[1].split(".")
							if len(domain_parts) > 0:
								normalized_lead_detail['domain'] = domain_parts[0].upper()
								normalized_lead_detail['domain_tld'] = domain_parts[-1]
						else:
							normalized_lead_detail['domain'] = email_parts[0].upper()
							normalized_lead_detail['domain_tld'] = email_parts[0].upper()
				except:
					secure_log( "Error reading email domain")	
					secure_log(normalized_lead['email_stated']   )

				domain_list = account_config["domains"]["list"]
			
				### Put all others in OTHER 
				
				if 'domain' in normalized_lead_detail: normalized_lead_detail['BIN_domain'] = normalized_lead_detail['domain']
				if 'BIN_domain' in normalized_lead_detail and normalized_lead_detail['BIN_domain'] not in domain_list: normalized_lead_detail['BIN_domain'] = 'OTHER'
				
				###############################
				###### Group tld domains ######
				###############################
				if 'domain_tld' in normalized_lead_detail: normalized_lead_detail['BIN_domain_tld'] = normalized_lead_detail['domain_tld']
				

				########################################
				# Update lead_date (lead_details.ialgo)
				########################################	
				if ('lead_datetime' in normalized_lead_detail):
					if (leads_software == 'velocify'):
						lead_date = datetime.datetime.strptime( normalized_lead_detail['lead_datetime'], '%Y-%m-%dT%H:%M:%S' ).date()
					else:
						lead_date = pd.to_datetime(normalized_lead_detail['lead_datetime']).date()

					normalized_lead_detail['lead_date'] = str(lead_date)


				#######################################
				# TODO: Clean this up later, these attributes have been removed from lead_details
				#	   and are now part of the encompass (los) table. Is it safe to remove them 
				#	   from the global_attribute_mapping table?
				#######################################
				encompass_attributes = ['coborrower_race',
										'borrower_marital_status',
										'borrower_email',
										'enc_loan_purpose',
										'loan_status',
										'days_in_process',
										'coborrower_home_phone',
										'coborrower_email',
										'milestone_date_submittal',
										'milestone_date_approval',
										'status_detail',
										'borrower_home_phone',
										'borrower_last_name',
										'enc_velocify_lead_id',
										'account_loan_id',
										'borrower_mailing_address',
										'enc_adverse_date',
										'coborrower_mailing_zip',
										'enc_last_action_date',
										'borrower_cell_phone',
										'coborrower_work_phone',
										'down_payment_per',
										'borrower_income',
										'closing_date',
										'coborrower_last_name',
										'veteran_servicestatus',
										'borrower_work_phone',
										'coborrscore_range_statedower_mailing_address',
										'enc_loan_folder',
										'coborrower_income',
										'coborrower_mailing_city',
										'enc_loan_amount',
										'coborrower_fico',
										'coborrower_cell_phone',
										'enc_app_date',
										'borrower_ethnicity',
										'enc_loan_officer',
										'coborrower_mailing_state',
										'purpose_description',
										'coborrower_first_name',
										'borrower_gender',
										'coborrower_gender',
										'milestone_date_funded',
										'loan_type',
										'loan_source',
										'borrower_first_name',
										'borrower_fico',
										'coborrower_ethnicity',
										'enc_file_started_date',
										'milestone_date_completion',
										'borrower_mailing_zip',
										'enc_nmls_id',
										'iapplication',
										'iclose',
										'ilock',
										'icontact',
										'iqualification',
										'ialgo',
										'home_value_corr',
										'loan_amount_corr',
										'ltv_calc',
										'milestone_date_contact',
										'milestone_date_qualification']

				bin_credit_result = get_bin_credit_profile(normalized_lead, account_config, credit_profile_lookup)
				if bin_credit_result is not None:
					normalized_lead_detail["bin_credit_profile_stated"]  = bin_credit_result

				for attr in encompass_attributes:
					if attr in normalized_lead_detail: 
						normalized_lead_detail.pop(attr, None) 

				############################################
				# iContact and iQualifictation
				# NOTE: This was moved to the view_external_lead_details Postgres view
				############################################
				
				normalized_lead_details.append( normalized_lead_detail )

			secure_log("::::: Done Processing Leads")
			secure_log("::::: Leads With No First Assignment: {}".format(Nno_firstassign))
			secure_log("::::: Leads With Profile ID Officer Default or Profile ID First Assignment User Not Found in Agent Profiles: {}".format(Nno_loan_officer))
			secure_log("::::: Leads With No User: {}".format(Nno_user))
			secure_log("::::: Leads With User Not Found in Agent Profiles: {}".format(Nno_profile_id_user))

			secure_log("::::: Missing Users: {}".format(len(missing_users)))
			secure_log("::::: Missing First Assignment Users: {}".format(len(missing_first_assignment_users)))

			if(is_data_pull is True):
				normalized_leads = sorted(normalized_leads, key=lambda k: k['account_lead_id']) 
				normalized_lead_details = sorted(normalized_lead_details, key=lambda k: k['account_lead_id'])
			
			
			producer = KafkaProducer(bootstrap_servers=[host],
						acks= config["acks"],
						retries= config["retries"],
						security_protocol='SASL_PLAINTEXT',
						sasl_mechanism='PLAIN',
						sasl_plain_username=config["user"],
						sasl_plain_password=k_pass,
						linger_ms= config['linger_ms'],
						batch_size= 32*1024 ,
						value_serializer=lambda x: dumps(x, default=str).encode('utf-8')
						)

			secure_log("::: Kafka Producer created :::")

			leads_avro_producer = KafkaAvroProducer('external_leads_ksql')
			for i, e in enumerate(normalized_leads):
				e['ksql_id'] = int(str(e['account_id']) + str(e['account_lead_id']))

				try:
					leads_avro_producer.send(e, e['account_id'])
				except BufferError:
					secure_log(":::: flushing {} leads out of{}".format(i,len(normalized_leads)))
					leads_avro_producer.flush()
					leads_avro_producer.send(e, e['account_id'])

				k = bytes(int(e['account_id']))

				try:
					p = producer.send(external_leads_topic, key=k, value=e).\
					add_callback(on_send_success).\
					add_errback(on_send_error)
				except BufferError:
					secure_log(":::: flushing {} leads out of{}".format(i,len(normalized_leads)))
					producer.flush()
					p = producer.send(external_leads_topic, key=k, value=e).\
					add_callback(on_send_success).\
					add_errback(on_send_error)


			
			secure_log(":::: {} external_lead Records sent to stream".format(len(normalized_leads)))

			# TODO: Turn this once schema is fixed
			#lead_details_avro_producer = KafkaAvroProducer('external_lead_details_ksql')
			for i, e in enumerate(normalized_lead_details):
				#e['ksql_id'] = int(str(e['account_id']) + str(e['account_lead_id']))

				#lead_details_avro_producer.send(e, e['account_id'])

				k = bytes(int(e['account_id']))
				p = producer.send(external_lead_details_topic, key=k, value=e).\
					add_callback(on_send_success).\
					add_errback(on_send_error)
					
			secure_log(":::: {} external_lead_detail Records sent to stream".format(len(normalized_lead_details)))

			 #Filter out leads with propair_ids
			leads_df = pd.DataFrame(normalized_leads)
			if ('propair_id' in list(leads_df.columns)):
				leads_df = leads_df.loc[pd.isnull(leads_df['propair_id']) == False][['account_id', 'account_lead_id', 'propair_id']]

				if (len(leads_df) > 0):
					secure_log("::::: Detected ProPair IDS. Sending ids to stream")
					leads_ids = json.loads(leads_df.to_json(orient='records', date_format='iso'))

					for i, e in enumerate(leads_ids):
						k = bytes(int(e['account_id']))
						p = producer.send('update_lead_ids', key=k, value=e).\
							add_callback(on_send_success).\
							add_errback(on_send_error)

					secure_log("::::: {} Records sent to Update Lead IDs stream".format(len(leads_df)))

			

			# Send to lockclose if configuration set
			if ('los' in account_config):
				send_to_lockclose = account_config['los']['match_type'] == 'velocify' if 'match_type' in account_config['los'] else False

				if (send_to_lockclose):
					secure_log("::: Los Matchtype = Velocify. Sending data to lockclose consumer :::")

					close_field = account_config['los']['date_close_field'] if 'date_close_field' in account_config['los'] else 'milestone_date_docsigning'
					
					leads_df = pd.DataFrame(normalized_lead_details)
					
					if (close_field in leads_df.columns):

						# Set default milestones
						milestones = ['milestone_date_lock', close_field]
						milestones = milestones + list(account_config['los']['milestones'].values()) if 'milestones' in account_config['los'] else milestones
						for mile in milestones:
							if mile in leads_df.columns:
								leads_df[mile] = pd.to_datetime(leads_df[mile],errors='coerce')
								leads_df[mile] =  [x if (x > pd.to_datetime('1900-01-01 00:00:00') and pd.isna(x) == False) else None for x in leads_df[mile]]


						leads_df = leads_df.dropna(how='all', subset=[x for x in milestones if x in leads_df.columns])

						if (len(leads_df) > 0):
							secure_log("::::: Uploading {} leads with milestones for lockclose".format(len(leads_df)))
							####################################
							# Upload to s3
							####################################
							gz_buffer = BytesIO()

							with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
								leads_df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)

							s3_resource = boto3.resource('s3')
							file_path = 'los/download/{}_los-{}.csv.gz'.format(account_meta['account'].lower(),datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S'))
							s3_object = s3_resource.Object(os.environ['BUCKET'], file_path)
							s3_object.put(Body=gz_buffer.getvalue())

							secure_log("::::: Succesfully Uploaded LOS file to s3. Bucket: {}, Path: {}".format(os.environ["BUCKET"],file_path))

							message_meta = {
								"account_id": account_meta['account_id'],
								"account_name": account_meta['account'],
								"bucket": os.environ["BUCKET"],
								"file_path": file_path
							}

							k = bytes(int(account_id))
							p = producer.send('los', key=k, value=message_meta)

							secure_log(":::: File Path sent to stream")
						else:		
							secure_log("::::: No locks or closes found")		
					else:
						secure_log("::::: Dynamo ERROR! Close Field: {} does not exist! ... Either no leads have a close OR global_attribute_lookup is missing the field".format(close_field))

			# block until all sync messages are sent
			producer.flush()
			#lead_details_avro_producer.flush()
			leads_avro_producer.flush()
			producer.close()
	   


		else:
			secure_log("::::: Report not found.")

		cur.close()
		conn.close()
		secure_log("::::: All Done.")
	except Exception as err:
		secure_log("::::: ERROR ", err)
		secure_log(traceback.format_exc())

		try:
			py_err = ProPairError(err, "Leads_Producer {}".format(scheduler_format), exec_info=sys.exc_info(), stack=traceback.format_exc())
		except:
			py_err = ProPairError(err, "Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())

		try:
			account_meta = json.loads(event['Records'][0]['Sns']['Message'])
			account = account_meta['account']
			py_err.account = account
			report_to_rollbar(rollbar_topic, py_err)
			conn.rollback()
		except Exception as err:
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)
			conn.rollback()
		

def sanitize_and_cast(val, to_data_type, field, accepted_fields):
	if(val == None):
		return None
	elif( "date" in to_data_type.lower() or "time" in to_data_type.lower() ):
		### Check if string is a malformed date ###
		#print( "DATE DEBUGGER")

		match = re.match(r"(^20\d{2}$)", val) #2005, 2015, 2021, etc
		if match and match.groups() and len(match.groups()) == 1:
			secure_log( "Formatted to: {}-01-01".format(val) )
			return "{}-01-01".format(val)

		match = re.match(r"(1/0/1900)", val) #1/0/1900
		if match and match.groups() and len(match.groups()) == 1:
			secure_log( "Formatted to: 1900-01-01")
			return "1900-01-01"

		match = re.match(r"(0\d{1}|1[0-2])([0-2]\d{1}|3[0-1])(20\d{2})", val) #06012012
		if match and match.groups() and len(match.groups()) == 3:
			secure_log( "Formatted to: {}-{}-{}".format(match.groups()[2], match.groups()[0], match.groups()[1]) )
			return "{}-{}-{}".format(match.groups()[2], match.groups()[0], match.groups()[1])

		return val
	elif( "char" in to_data_type.lower() ):
		#TODO: Sanitize before casting
		try:
			if(str(val).upper() == "NAN" or str(val).upper() == "NA" or str(val).upper() == "None"):
				return None
			else:	
				return re.sub('\$\$', '', str(val).encode('ascii','ignore').decode('unicode_escape'))

		except Exception as e:
			secure_log("::::: String sanitization error! Setting to blank. Field: {}, Value: {}".format(field, val))
			secure_log(e)
			return ''
			
	elif( "int" in to_data_type.lower()):
		#TODO: Sanitize before casting
		if(isinstance(val, str)):
			if(val.lower() == 'true' or val.lower() == 'yes'):
				return 1
			elif(val.lower() == 'false' or val.lower() == 'no'):
				return 0
			else:
				numvalue = None
				orig_val = val

				if 'Down' not in orig_val:
					numvalue = re.sub("[^eE0-9.+-]", "", str(val))
				else:
					secure_log('ERROR: (DOWN in val): field,orig_val,val = {},{},{}'.format(field,orig_val,val))
					numvalue = None 

				if (numvalue is not None):
					try:
						if field in accepted_fields:
							x = float(re.sub("[^\d]", "", str(numvalue)))
							if int(x) > 100000000:
								secure_log('(SPLITTING DATA) field, value = {}, {}'.format(field, numvalue))
								numvalue = ((int(str(int(x))[0:len(str(int(x)))//2]) + int(str(int(x))[len(str(int(x)))//2:len(str(int(x)))]))//2)
								secure_log('(POST SPLIT): {}'.format(numvalue))
							else: 
								numvalue = x
						else:
							numvalue = re.sub("(?<!^)[+-]", "", str(numvalue))
					except Exception as e:
						secure_log('ERROR (numvalue): field, orig_val, numvalue = {},{},{} '.format(field,orig_val, numvalue))
						secure_log(e)

				if(numvalue is not None):
					try:
						final_val = int( float( numvalue ) )

						#If phone numer is greater than 10 digits 
						#Set to -2. Identifying it as a bad phone number
						if "phone" in field and final_val > 100000000000:
							final_val = -2

						return final_val
					except Exception as e:
						secure_log('ERROR (numvalue is not None): {},{}'.format(val,e))
						return -1
				else:
					secure_log('ERROR: numvalue is None')
					secure_log( "Could not parse {} to {}.".format(val, to_data_type) )
					return -1
		else:
			numvalue = val
			if (numvalue is not None):
					try:
						if field in accepted_fields:
							x = float(re.sub("[^\d]", "", str(numvalue)))
							if int(x) > 100000000:
								secure_log('(SPLITTING DATA) field, value = {}, {}'.format(field, numvalue))
								numvalue = ((int(str(int(x))[0:len(str(int(x)))//2]) + int(str(int(x))[len(str(int(x)))//2:len(str(int(x)))]))//2)
								secure_log('(POST SPLIT): {}'.format(numvalue))
							else: 
								numvalue = x
						else:
							numvalue = re.sub("(?<!^)[+-]", "", str(numvalue))
					except Exception as e:
						secure_log('ERROR (numvalue): field, orig_val, numvalue = {},{},{} '.format(field,orig_val, numvalue))
						secure_log(e)

			return int(numvalue)

	elif("float" in to_data_type.lower() or "double" in to_data_type.lower()):
		#TODO: Sanitize before casting
		try:
			return_val = float(val)
		except:
			try:
				return_val = float(re.sub("[^0-9.-]", "", str(val)))
			except:
				return_val = None
		
		return return_val

	else:
		return val
			