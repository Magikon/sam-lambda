
import os
import sys
import pdb
import re
import pandas as pd
import numpy as np
import time
import requests
import json
import psycopg2 as postdb
import datetime as dt
import traceback
import argparse
import boto3
import yaml
from boto3 import Session
from ricochet import Ricochet

from kafka import KafkaProducer
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.customErrors import ProPairError, report_to_rollbar

from utils import sanitize_field, getAgentMapping, get_lookup, get_bin_credit_profile

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

def str2bool(v):
	if isinstance(v, bool):
		return v
	if v.lower() in ('yes', 'true', 't', 'y', '1'):
		return True
	elif v.lower() in ('no', 'false', 'f', 'n', '0'):
		return False
	else:
		raise argparse.ArgumentTypeError('Boolean value expected.')

parser = argparse.ArgumentParser(description="Pull Ricochet Historical Data")
parser.add_argument("--account_id", required=True, type=int, help="Account ID")
parser.add_argument("--s3_bucket", required=True, default=None, type=str, help="S3 Bucket: <bucket_name>", metavar="<bucket_name>")
parser.add_argument("--load_data", default=False, type=str2bool, help="Load S3 Data into Redshift")
parser.add_argument("--create_producer", default=False, type=str2bool, help="Load S3 Data into Redshift")
parser.add_argument("--start_page", default=1,type=int)
parser.add_argument("--end_page", default=3,type=int)
args = vars(parser.parse_args())

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

rollbar_topic = os.environ["TOPIC_ROLLBAR"]

config = yaml.safe_load( open('./config/config.yml'))
config = config[os.environ['ENV']]
host= config["host"]

def meld_columns(rows, columns):
	result = []
	for row in rows:
		result.append(dict(zip(columns, row)))
	return result

def on_send_success(record_metadata):
	pass
	#secure_log('{0} {1}  {2}'.format(record_metadata.topic,record_metadata.partition, record_metadata.offset) )

def on_send_error(excp):
	secure_log('Something went wrong: ', excp)
	
	py_err = None
	try:
		if isinstance(excp, str):
			py_err = ProPairError(excp, "Ricochet_Pull", exec_info=sys.exc_info(), stack=traceback.format_exc())
		else:
			py_err = ProPairError(excp.message, "Ricochet_Pull", exec_info=sys.exc_info(), stack=traceback.format_exc())

	except Exception as e:
		secure_log(e)
		py_err = ProPairError(str(excp), "Ricochet_Pull", exec_info=sys.exc_info(), stack=traceback.format_exc())
	py_err.account = ACCOUNT if ACCOUNT is not None and ACCOUNT != "" else "NO_ACCOUNT"
	report_to_rollbar(rollbar_topic, py_err)

def get_column_data(table_name, cur):

	sq = "SELECT \"column\",type from pg_table_def where tablename='{}'".format(table_name)
	count = 0
	successful = False

	while not successful and count < 2:
		try:
			cur.execute(sq)
			data = cur.fetchall()
			if (len(data) > 0):
				successful = True
			else:
				raise Exception("No column data found")
		except Exception as e:
			secure_log("::::: [{0}] Error - {1} trying again...".format(table_name, e))
			secure_log("::::: [{0}] Num of retries: {1}".format(table_name, count))
			sleep(0.5*count)

		count += 1
	
	if (successful):
		secure_log("::::: Found data types")

		column_data = {}
		for line in data:
			column_data[line[0]] = {"name": line[0], "type": line[1]}

		return column_data
	else:
		raise Exception("[{0}] After {1} retries, was unable to retrieve column_data".format(table_name, count))

def get_unique_agents(leads, account_id, agent_ids = []):
	agents = []
	for lead in leads:
		if 'currentowner' in lead:
			try:
				user = lead['currentowner']
			except Exception as e:
				print("JSON PARSING ERROR: ", e)
				user = None

			if (user):
				user_id = int(user['id'])
				cur_date = dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
				agent = {
					"account_id": account_id,
					"agent_id": user_id,
					"first_name": user['firstName'],
					"last_name": user['lastName'],
					"name": '{} {}'.format(user['firstName'], user['lastName']), 
					"name_velocify": '{}, {}'.format(user['lastName'], user['firstName']), 
					"created_at": cur_date, 
					"updated_at": cur_date
				}
					
				if user_id not in agent_ids:
					agent_ids.append(user_id)
					agents.append(agent)

	return agents

def insert_agent_profiles(account_id, leads):
	agents = get_unique_agents(leads, account_id)

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
					value_serializer=lambda x: json.dumps(x).encode('utf-8')
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

def get_global_attr(cur, account, table_name):
	secure_log("::::: Fetching Global Attribute Mapping {} for account {}".format(table_name, account['name']))

	try:		
		query = """
			SELECT propair_field, customer_field_name, customer_original_field_name, customer_split, datatype FROM global_attribute_lookup 
			WHERE account_id = {0} AND (table_name='{1}' OR table_name ='all') AND account_system = 'Velocify' and include_variable = 1
			AND ( customer_field_name IS NOT NULL OR customer_original_field_name IS NOT NULL OR customer_split IS NOT NULL );
			""".format(account['id'], table_name)

		cur.execute(query)
		response = meld_columns(cur.fetchall(), [x.name for x in cur.description])

		if (len(response) > 0):
			return response
		else:
			raise ValueError("Could not find Account -> {} in global_attribute lookup".format(account['name']))

	except Exception as err:
		secure_log("ERROR: ", err)
		py_err = ProPairError(err, "Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		try:
			py_err.account = account['name']
			report_to_rollbar(rollbar_topic, py_err)
		except Exception as err2:
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)

def get_token(account): 
	client = boto3.client('ssm')
	response = client.get_parameter(
		Name="/{}/{}/ricochet-token".format(os.environ["ENV"], account['name']), 
		WithDecryption=True
	) 
	return response['Parameter']['Value']

def get_account(cur, id):
	sql = "SELECT * FROM accounts where id = '{}'".format(id)
	cur.execute(sql)
	response = meld_columns(cur.fetchall(), [x.name for x in cur.description])
	if (len(response) > 0):
		return response[0]
	else:
		raise ValueError("Could not find account {} in database".format(id))

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
			
	elif( "int" in to_data_type.lower() or "FLOAT" in to_data_type or "DOUBLE" in to_data_type):
		#TODO: Sanitize before casting
		if(isinstance(val, str)):
			if(val == 'True' or val == 'YES'):
				return 1
			elif(val == 'False' or val == 'NO'):
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
							secure_log('(SPLITTING DATA) field, value = {}, {}'.format(field, numvalue))
							x = float(re.sub("[^\d]", "", str(numvalue)))
							numvalue = ((int(str(int(x))[0:len(str(int(x)))//2]) + int(str(int(x))[len(str(int(x)))//2:len(str(int(x)))]))//2) if int(x) > 100000000 else x
							secure_log('(POST SPLIT): {}'.format(numvalue))
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
			return val
	else:
		return val


def map_fields(attr_map, lead, account, columns):

	accepted_fields = [
		"purchase_price_stated",
		"home_value_stated",
		"loan_amount_stated",
		"down_payment",
		"cash_out_stated"
	]

	normalized_lead = {"account_id": account['id']}

	for attribute in attr_map:
		datatype = columns[attribute['propair_field']] if attribute['propair_field'] in columns else attribute['datatype'] 
		#TODO: Handle ':' string splits
		if attribute['customer_original_field_name'] in lead:
			normalized_lead[ attribute['propair_field'] ] = sanitize_and_cast( lead[ attribute['customer_original_field_name'] ], datatype['type'], attribute['propair_field'], accepted_fields)
		elif ':' in attribute['customer_field_name']: 
			components = attribute['customer_field_name'].split(':')
			if components[0] in lead:
				values = lead[ components[0] ].split(attribute['customer_split'])
				
				if(len(values) > int(components[1])):
					normalized_lead[ attribute['propair_field'] ] = sanitize_and_cast( values[ int(components[1]) ], datatype['type'], attribute['propair_field'], accepted_fields)
		elif attribute['customer_field_name'] in lead: 
			normalized_lead[ attribute['propair_field'] ] = sanitize_and_cast( lead[ attribute['customer_field_name'] ], datatype['type'], attribute['propair_field'], accepted_fields)

	return normalized_lead

def normalize_leads( leads, account, account_config, profile_dict, cur):
	normalized_leads = []
	normalized_lead_details = []

	attribute_map = get_global_attr(cur, account, 'lead')
	detail_attribute_map = get_global_attr(cur, account, 'lead_detail')
	credit_profile_lookup = get_lookup(account['id'], cur)
	agent_mapping = getAgentMapping(cur, account['name'], account['id'])

	l_csv_columns = get_column_data('external_leads', cur)
	ld_csv_columns = get_column_data('external_lead_details', cur)

	for lead in leads:

		#Sanitizing Field Names for Mapping
		new_lead = lead.copy()
		Ncol = len(new_lead.keys())
		for key, value in lead.items():
			if (sanitize_field(key) != key):
				new_lead[sanitize_field(key)] = lead[key]
				del new_lead[key]

		lead = new_lead.copy()
	
		normalized_lead = map_fields(attribute_map, lead, account, l_csv_columns)
		normalized_lead_detail = map_fields(detail_attribute_map, lead, account, ld_csv_columns)
		
		#### Cleanup Zip Codes.
		try:
			if 'property_zip_code_stated' in normalized_lead and normalized_lead['property_zip_code_stated'] is not None and len(normalized_lead['property_zip_code_stated']) > 5:
				normalized_lead['property_zip_code_stated'] = normalized_lead['property_zip_code_stated'][:5]
		except Exception as e:
			secure_log("Error cleaning up zip code: {}".format(e))

		#################################
		# Create lookup to profile table
		#################################
		
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
			normalized_lead_detail['profile_id_loan_officer_default'] = profile_dict[normalized_lead_detail['account_lead_id']]['profile_id_loan_officer_default']
			normalized_lead_detail['profile_id_user'] = profile_dict[normalized_lead_detail['account_lead_id']]['profile_id_user']
			normalized_lead_detail['profile_id_first_assignment_user'] = profile_dict[normalized_lead_detail['account_lead_id']]['profile_id_first_assignment_user']
	
		######################################
		# Add Originated Loans
		######################################
		normalized_lead_detail['second_loan'] = 0
		
		########################################################
		# Grab lead domain 
		########################################################

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
		try: 
			lead_date = dt.datetime.strptime( normalized_lead_detail['lead_datetime'], '%Y-%m-%dT%H:%M:%S' ).date()
		except:
			try:
				lead_date = dt.datetime.strptime( normalized_lead_detail['lead_datetime'], '%Y-%m-%d %H:%M:%S' ).date()
			except Exception as e:
				continue;

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
		
		normalized_leads.append( normalized_lead )
		normalized_lead_details.append( normalized_lead_detail )

	return normalized_leads, normalized_lead_details, l_csv_columns, ld_csv_columns

def main():
	try: 
		account_id	= int(args['account_id'])
		#start_dt           = dt.datetime.strptime(account_meta['start_date'],"%Y-%m-%d %H:%M:%S") if 'start_date' in account_meta else None
		#end_dt 			   = dt.datetime.strptime(account_meta['end_date'],"%Y-%m-%d %H:%M:%S") if 'end_date' in account_meta else dt.datetime.now()
		
		account_config = get_account_config(account_id) 
		account_config['name']
		secure_log("::::: Initializing Data Architecture Ricochet", configuration=account_config)

		start_page = int(args['start_page'])
		end_page = int(args['end_page'])

		secure_log("::::: Connecting to database")
		conn = postdb.connect('host={} dbname={} port={} user={} password={}'.format(db_host,db_name,db_port,db_user,db_pass))
		cur = conn.cursor()

		account = get_account(cur, account_id)

		s3_bucket = args['s3_bucket']
		load_data = args['load_data']
		create_producer = args['create_producer']

		##################r
		# Grab lead list
		###################
		rcc = Ricochet(conn, {'id': account_id, 'name': account_config['name']})
		GRAB_LEAD_LIST = True
		if GRAB_LEAD_LIST:
			all_leads,all_actions = rcc.get_lead_action_list('id','asc',start_page,end_page)
		else:
			total_leads = 50000

			leads = [12703351, 12703352, 12703353, 12703354, 12703355, 12703356, 12703357, 12703358, 12703359, 12703360, 12703361, 12703362, 12703363, 12703364, 12703365, 12703367, 12703368, 12703370, 12703371, 12703372, 12703373, 12703374, 12703375, 12703376, 12703377, 12703378, 12703379, 12703380, 12703381, 12703382, 12703383, 12703384, 12703385, 12703386, 12703387, 12703388, 12703389, 12703390, 12703391, 12703392, 12703393, 12703394, 12703395, 12703396, 12703397, 12703398, 12703399, 12703400, 12703401, 12703402, 13441551, 13441552, 13441553, 13441554, 13441555, 13441556, 13441557, 13441558, 13441559, 13441560, 13441561, 13441562, 13441563, 13441564, 13441565, 13441566, 13441567, 13441568, 13441569, 13441570, 13441571, 13441572, 13441573, 13441574, 13441575, 13441576, 13441577, 13441578, 13441579, 13441580, 13441581, 13441582, 13441583, 13441584, 13441585, 13441586, 13441587, 13441588, 13441589, 13441590, 13441591, 13441592, 13441593, 13441594, 13441595, 13441596, 13441597, 13441598, 13441599, 13441600, 13441601, 13441602, 13441603, 13441604, 13441605, 13441606, 13441607, 13441608, 13441609, 13441610, 13441611, 13441612, 13441613, 13441614, 13441615, 13441616, 13441617, 13441618, 13441619, 13441620, 13441621, 13441622, 13441623, 13441624, 13441625, 13441626, 13441627, 13441628, 13441629, 13441630, 13441631, 13441632, 13441633, 13441634, 13441635, 13441636, 13441637, 13441638, 13441639, 13441640, 13441641, 13441642, 13441643, 13441644, 13441645, 13441646, 13441647, 13441648, 13441649, 13441650, 13441651, 13441652, 13441653, 13441654, 13441655, 13441656, 13441657, 13441658, 13441659, 13441660, 13441661, 13441662, 13441663, 13441664, 13441665, 13441666, 13441667, 13441668, 13441669, 13441670, 13441671, 13441672, 13441673, 13441674, 13441675, 13441676, 13441677, 13441678, 13441679, 13441680, 13441681, 13441682, 13441683, 13441684, 13441685, 13441686, 13441687, 13441688, 13441689, 13441690, 13441691, 13441692, 13441693, 13441694, 13441695, 13441696, 13441697, 13441698, 13441699, 13441700]
 
			all_leads = [] 
			for c in leads:
				d = {}
				d['id'] = c
				all_leads.append(d)

		##################################################
		# Extract all actions (events) from ricochet logs
		##################################################
		profile_dict = rcc.get_event_logs(all_actions)

		#print(profile_dict)
		#print('Leads, Profiles . {},{}'.format(len(all_leads),len(profile_dict)))

		#############################################
		# Grab data from each individual lead 
		#############################################

		secure_log('Total Leads, Unique Leads . {},{}'.format(len(sorted([x['id'] for x in all_leads])),len(set(sorted([x['id'] for x in all_leads])))))
		print(sorted([x['id'] for x in all_leads]))
		all_data = rcc.get_lead_data(all_leads)

		#Inserting agents
		try:
			insert_agent_profiles(account_id, all_data)
		except Exception as err:
			py_err = ProPairError(err.message, "Ricochet_Pull", exec_info=sys.exc_info(), stack=traceback.format_exc())
			py_err.account = account['name'] if account is not None else "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)

		secure_log("::::: Normalizing Records")

		normalized_leads, normalized_lead_details, l_csv_columns, ld_csv_columns = normalize_leads(all_data, account, account_config, profile_dict, cur)

		if(s3_bucket is not None):
			secure_log("::::: Uploading results to S3")
			bucket = s3_bucket

			def index_marks(nrows, chunk_size):
				return range(1 * chunk_size, (nrows // chunk_size + 1) * chunk_size, chunk_size)

			def split(dfm, chunk_size):
				indices = index_marks(dfm.shape[0], chunk_size)
				return np.split(dfm, indices)

			def cast_df(df, columns):
				for i, v in columns.items():
					col = v['name']
					dtype = v['type']

					if col in list(df.columns):
						if ('time' in dtype):
							print("::::: Setting {} to datetime".format(col))
							df[col] = pd.to_datetime(df[col])
						if ('int' in dtype):
							print("::::: Setting {} to int".format(col))
							df[col] = [int(x) if pd.isnull(x) == False else -1 for x in df[col]]
							df[col] = [x if x != -1 else np.nan for x in df[col]]
							df[col] = df[col].astype('Int64')
							#print(df[col])
							#df[col] = pd.Series([str(x) for x in df[col]]).astype('Int64')
							#df[col] = df[col].astype('Int64')
				return df

			def copy_to_redshift(cur, conn, df, iam, bucket, table_name, file_name, account):
				secure_log("::::: Copying to {} . {}".format(table_name, file_name))
				sql = """
					copy {table} ({columns}) 
					from 's3://{bucket}/ricochet/{account}/{table}/{file_name}'
					iam_role 'arn:aws:iam::{iam}:role/pp-redshift-role'
					IGNOREHEADER 1
					EMPTYASNULL
					ACCEPTINVCHARS
					TRUNCATECOLUMNS
					DELIMITER AS '|'
					csv
					gzip;
					""".format(iam=iam, table=table_name, bucket=bucket, account=account['name'], columns=','.join(list(df.columns)),file_name=file_name.split('_c')[0])
				cur.execute(sql)
				conn.commit()

			def upload_to_s3(df, table_name, account, csv_date, start_page, end_page, Nchunk):
				file_name = "{}_p{}_to_p{}_l{}_to_l{}_c{}.csv.gz".format(table_name, str(start_page).zfill(5),str(end_page).zfill(5),\
							str((start_page-1)*50+1).zfill(6), str((end_page-1)*50).zfill(6),str(Nchunk).zfill(3) )

				s3 = boto3.resource('s3')
				df.to_csv(file_name, index=False, sep="|", compression='gzip')

				secure_log("::::: CSV LOCATION: {}".format(file_name))
				
				secure_log("::::: Uploading {} to S3".format(file_name))
				s3.meta.client.upload_file(file_name, bucket, 'ricochet/{}/{}/{}'.format(account['name'].lower(), table_name, file_name))

				return file_name

			leads_df = cast_df(pd.DataFrame(normalized_leads), l_csv_columns)
			lead_details_df = cast_df(pd.DataFrame(normalized_lead_details), ld_csv_columns)

			csv_date = dt.datetime.now().strftime("%Y-%m-%d_%H:%M:%S")

			l_chunks = split(leads_df, 1000)
			Nchunk = 0
			for c in l_chunks:
				Nchunk = Nchunk + 1
				if (len(c) > 0):
					external_leads_filename = upload_to_s3(c, 'external_leads', account, csv_date, start_page, end_page, Nchunk)

			ld_chunks = split(lead_details_df, 1000)
			Nchunk = 0
			for c in ld_chunks:
				Nchunk = Nchunk + 1
				if (len(c) > 0):
					external_lead_details_filename = upload_to_s3(c, 'external_lead_details', account, csv_date, start_page, end_page, Nchunk)
			
			secure_log("::::: Done")

			if (load_data):
				create_producer = False
				session = Session()
				c = session.client("sts")
				current_user = c.get_caller_identity()
				iam = current_user['Account']

				copy_to_redshift(cur, conn, leads_df, iam, bucket, 'external_leads', external_leads_filename, account)
				copy_to_redshift(cur, conn, lead_details_df, iam, bucket, 'external_lead_details', external_lead_details_filename, account)
		
		if (create_producer):
			producer = KafkaProducer(bootstrap_servers=[host],
						acks= config["acks"],
						retries= config["retries"],
						security_protocol='SASL_PLAINTEXT',
						sasl_mechanism='PLAIN',
						sasl_plain_username=config["user"],
						sasl_plain_password=k_pass,
						linger_ms= config['linger_ms'],
						batch_size= 32*1024 ,
						value_serializer=lambda x: json.dumps(x).encode('utf-8')
						)

			secure_log("::: Producer created :::")

			for i, e in enumerate(normalized_leads):
				k = bytes(int(e['account_id']))
				p = producer.send('external_leads', key=k, value=e).\
					add_callback(on_send_success).\
					add_errback(on_send_error)
			
			secure_log(":::: {} external_lead Records sent to stream".format(len(normalized_leads)))

			for i, e in enumerate(normalized_lead_details):
				k = bytes(int(e['account_id']))
				p = producer.send('external_lead_details', key=k, value=e).\
					add_callback(on_send_success).\
					add_errback(on_send_error)

			secure_log(":::: {} external_lead_detail Records sent to stream".format(len(normalized_lead_details)))

			producer.flush()
			producer.close()

		cur.close()
		conn.close()
		secure_log("::::: All Done.")

	except Exception as err:
		print("ERROR: ", err)
		py_err = ProPairError(err, "Ricochet_Pull", exec_info=sys.exc_info(), stack=traceback.format_exc())
		try:
			py_err.account = account['name']
			report_to_rollbar(rollbar_topic, py_err)
		except Exception as err:
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)


if __name__ == "__main__":
	main()
