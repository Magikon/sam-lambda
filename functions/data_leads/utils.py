import sys
import os
import traceback
import requests
import datetime as dt
import re
import boto3
import pytz
import xmltodict
import yaml 
import json
import pandas as pd

from time import sleep
from json import dumps

from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.customErrors import ProPairError, report_to_rollbar

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


def get_unique_agents_ricochet(leads, account_meta, agent_ids = []):
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
					"account_id": account_meta['account_id'],
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
	if priority_list is None:
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
	
def meld_columns(rows, columns):
	result = []
	for row in rows:
		result.append(dict(zip(columns, row)))
	return result

def get_url(url, header):
    error = None
    r = None

    try:
        r = requests.get(url,headers=header)
    except Exception as e:
        try:
            print('RETRY 1 . requests.post failure . {}'.format(e))
            r = requests.get(url,headers=header)
        except Exception as e:
            try:
                print('RETRY 2 . requests.post failure . {}'.format(e))
                r = requests.get(url,headers=header)
            except Exception as e:
                print('requests.post failure . {}'.format(e))
                error = e
    
    return r, error

def sanitize_field(field):
	return re.sub("_+x00..", "", field)

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

def get_account(cur, id):
	sql = "SELECT * FROM accounts where id = '{}'".format(id)
	cur.execute(sql)
	response = meld_columns(cur.fetchall(), [x.name for x in cur.description])
	if (len(response) > 0):
		return response[0]
	else:
		raise ValueError("Could not find account {} in database".format(id))

def map_fields(attr_map, leads, account, columns):
	result = []
	
	accepted_fields = [
		"purchase_price_stated",
		"home_value_stated",
		"loan_amount_stated",
		"down_payment",
		"cash_out_stated"
	]

	for lead in leads:
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
		
		result.append(normalized_lead)

	return result

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
