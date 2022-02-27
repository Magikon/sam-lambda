
from faker import Faker
from collections import defaultdict
from datetime import datetime, timedelta

import os
import sys
import math
import re
import random
import xmltodict
import psycopg2
import requests 
import boto3
import traceback

fake = Faker()

from helpers.lead import LeadGenerator
from helpers.lookups import Lookups
from pplibs.logUtils import get_account_config
from utils import meld_columns
from utils import group_by_column
from db_utils import insert_to_db

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]),
	WithDecryption=True
) 

db_host = os.environ["REDSHIFT_ENDPOINT"]  # PRODUCTION

db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = os.environ["REDSHIFT_DB_PORT"]
db_pass = response['Parameter']['Value']

def get_agents(cur):
	sql = """
		SELECT A.id as propair_id, A.account_id, B.name as account_name, A.agent_id, A.name_velocify from agent_profiles A
		LEFT JOIN accounts B on A.account_id=B.id;
	"""
	cur.execute(sql)
	data = meld_columns(cur.fetchall(), [x.name for x in cur.description])

	return group_by_column(data, 'account_name')

def sanitize_and_cast(val, to_data_type, field, accepted_fields):
	if(val == None or val == ''):
		return None
	elif( "DATE" in to_data_type ):
		### Check if string is a malformed date ###
		#print( "DATE DEBUGGER")

		match = re.match(r"(^20\d{2}$)", val) #2005, 2015, 2021, etc
		if match and match.groups() and len(match.groups()) == 1:
			print( "Formatted to: {}-01-01".format(val) )
			return "{}-01-01".format(val)

		match = re.match(r"(1/0/1900)", val) #1/0/1900
		if match and match.groups() and len(match.groups()) == 1:
			print( "Formatted to: 1900-01-01")
			return "1900-01-01"

		match = re.match(r"(0\d{1}|1[0-2])([0-2]\d{1}|3[0-1])(20\d{2})", val) #06012012
		if match and match.groups() and len(match.groups()) == 3:
			print( "Formatted to: {}-{}-{}".format(match.groups()[2], match.groups()[0], match.groups()[1]) )
			return "{}-{}-{}".format(match.groups()[2], match.groups()[0], match.groups()[1])

		return val
	elif( "CHAR" in to_data_type ):
		#TODO: Sanitize before casting
		try:
			val = str(val)
			if(val.upper() == "NAN" or val.upper() == "NA" or val.upper() == "None"):
				return None
			else:	
				return val.encode('ascii','ignore').decode('unicode_escape')

		except Exception as e:
			print("::::: String sanitization error! Setting to blank. Field: {}, Value: {}".format(field, val))
			print(e)
			return ''
			
	elif( "INT" in to_data_type or "FLOAT" in to_data_type or "DOUBLE" in to_data_type):
		#TODO: Sanitize before casting
		if(isinstance(val, str)):
			if(val == 'True' or val == 'YES'):
				return 1
			elif(val == 'False' or val == 'NO'):
				return 0
			else:
				numvalue = None
				orig_val = val

				#print('BEFORE: field,val = {},{}'.format(field,val))
				# TODO: Handle 35%Down,35%Down etc use case
				# if ',' in val and 'Down' in val: 
				#	 numvalue = val.split(',')[0] 
				# else: numvalue = val
				#if 'Zero' in val:
				#	 numvalue = "0"
				#elif '5%' in val:
				#	 numvalue = "5"
				#elif 'More Than 50%' in val:
				#	 numvalue = "50"
				if 'Down' not in orig_val:
					numvalue = re.sub("[^eE0-9.+-]", "", str(val))
				else:
					print('ERROR: (DOWN in val): field,orig_val,val = {},{},{}'.format(field,orig_val,val))
					numvalue = None 

				#print('BEFORE SPLIT: field,val,numvalue = {},{},{}'.format(field,val,numvalue))
				if (numvalue is not None):
					try:
						if field in accepted_fields:
							x = float(re.sub("(?<!^)[+-]", "", str(numvalue)))
							numvalue = ((int(str(int(x))[0:len(str(int(x)))//2]) + int(str(int(x))[len(str(int(x)))//2:len(str(int(x)))]))//2) if int(x) > 100000000 else x
						else:
							numvalue = re.sub("(?<!^)[+-]", "", str(numvalue))
					except Exception as e:
						print('ERROR (numvalue): field,orig_val,val,numvalue = {},{},{},{}'.format(field,orig_val,val,numvalue))
						print(e)

				#print('AFTER SPLIT:  field,val,numvalue = {},{},{}'.format(field,val,numvalue))
				
				if 'Down' in orig_val:
					print('AFTER: field,orig_val,val,numvalue = {},{},{},{}'.format(field,orig_val,val,numvalue))

				if(numvalue is not None):
					try:
						final_val = int( float( numvalue ) )

						#If phone numer is greater than 10 digits 
						#Set to -2. Identifying it as a bad phone number
						if "phone" in field and final_val > 100000000000:
							final_val = -2

						return final_val
					except Exception as e:
						print('ERROR (numvalue is not None): {},{}'.format(val,e))
						return -1
				else:
					print('ERROR: numvalue is None')
					print( "Could not parse {} to {}.".format(val, to_data_type) )
					return -1
		else:
			return val
	else:
		return val

def main():
	try: 
		account = os.environ['ACCOUNT']
		days_ago = os.environ['DAYS_AGO']
		lead_interval = int(os.environ['LEAD_INTERVAL'])

		print("::::: Starting lead generation")
		print("::::: Account: {}".format(account))
		print("::::: Days ago: {}".format(days_ago))
		print("::::: Interval: {}".format(lead_interval))

		print('::::: Connecting to database')
		conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()
		print('::::: Successfully connected to database')

		sql = """
			SELECT id, name from accounts where name='{}'
		""".format(account)
		cur.execute(sql)

		account_id = cur.fetchone()
		if (account_id):
			account_id = account_id[0]
			account_config = get_account_config(account_id, cache=False)

			generator = LeadGenerator(cur)


			start_dt = datetime.now() - timedelta(days=int(days_ago))
			end_dt = datetime.now()

			leads = generator.generate_leads(account, start_dt=start_dt, end_dt=end_dt, max_interval=lead_interval)

			ls = Lookups(cur, generator.accounts)

			attribute_map = ls.get_global_attr('lead')[account]
			detail_attribute_map = ls.get_global_attr('lead_detail', include_variable=True)[account]

			agents = get_agents(cur)[account]

			normalized_leads = []
			normalized_lead_details = []

			for lead in leads:
				normalized_lead = { 'account_id': lead['CustomerId'] }
				normalized_lead_detail = { 'account_id': lead['CustomerId'] }
				accepted_fields = account_config['accepted_split_fields'] if 'accepted_split_fields' in account_config else []

				for attribute in attribute_map:
					if ':' in attribute['customer_field_name']: 
						components = attribute['customer_field_name'].split(':')
						if components[0] in lead:
							values = lead[ components[0] ].split(attribute['customer_split'])
							if(len(values) > int(components[1])):
								normalized_lead[ attribute['propair_field'] ] = sanitize_and_cast( values[ int(components[1]) ], attribute['datatype'], attribute['propair_field'], accepted_fields )
					elif attribute['customer_original_field_name'] in lead: 
						normalized_lead[ attribute['propair_field'] ] = sanitize_and_cast( lead[ attribute['customer_original_field_name'] ], attribute['datatype'], attribute['propair_field'], accepted_fields )
					elif attribute['customer_field_name'] in lead: 
						normalized_lead[ attribute['propair_field'] ] = sanitize_and_cast( lead[ attribute['customer_field_name'] ], attribute['datatype'], attribute['propair_field'], accepted_fields )
				
				
				#### Cleanup Zip Codes
				if 'property_zip_code_stated' in normalized_lead and normalized_lead['property_zip_code_stated'] is not None and len(normalized_lead['property_zip_code_stated']) > 5:
					normalized_lead['property_zip_code_stated'] = normalized_lead['property_zip_code_stated'][:5]


				normalized_lead_detail['profile_id_loan_officer_default']   = random.choice(agents)['propair_id']
				normalized_lead_detail['profile_id_first_assignment_user']  = normalized_lead_detail['profile_id_loan_officer_default'] 
				normalized_lead_detail['profile_id_user']				   = random.choice(agents)['propair_id'] if random.random() < 0.2 else normalized_lead_detail['profile_id_loan_officer_default']  

				normalized_leads.append( normalized_lead )

				for attribute in detail_attribute_map:
					#TODO: Handle ':' string splits
					if attribute['customer_original_field_name'] in lead:
						normalized_lead_detail[ attribute['propair_field'] ] = sanitize_and_cast( lead[ attribute['customer_original_field_name'] ], attribute['datatype'], attribute['propair_field'], accepted_fields )
					elif ':' in attribute['customer_field_name']: 
						components = attribute['customer_field_name'].split(':')
						if components[0] in lead:
							values = lead[ components[0] ].split(attribute['customer_split'])
							
							if(len(values) > int(components[1])):
								normalized_lead_detail[ attribute['propair_field'] ] = sanitize_and_cast( values[ int(components[1]) ], attribute['datatype'], attribute['propair_field'], accepted_fields )
					elif attribute['customer_field_name'] in lead: 
						normalized_lead_detail[ attribute['propair_field'] ] = sanitize_and_cast( lead[ attribute['customer_field_name'] ], attribute['datatype'], attribute['propair_field'], accepted_fields )

				
				
				normalized_lead_detail['profile_id_qualification']		  = None
				normalized_lead_detail['profile_id_processor']			  = None
				normalized_lead_detail['profile_id_sales_manager']		  = None
				normalized_lead_detail['profile_id_underwriter']			= None
				normalized_lead_detail['profile_id_encompass']			  = None
				
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
					print( "Error reading email domain")	
					print(normalized_lead['email_stated']   )

				domain_list = account_config['domains']["list"]
			
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
				lead_date = datetime.strptime( normalized_lead_detail['lead_datetime'], '%Y-%m-%dT%H:%M:%S' ).date()
				normalized_lead_detail['lead_date'] = str(lead_date)
				normalized_lead_detail['first_assignment_date'] = str(datetime.strptime( normalized_lead_detail['lead_datetime'], '%Y-%m-%dT%H:%M:%S' ) + timedelta(minutes=random.randint(1,15)))
				normalized_lead_details.append( normalized_lead_detail )

			uniq = ['account_id', 'account_lead_id']
			insert_to_db(normalized_leads, 'external_leads', cur, conn, uniq)
			insert_to_db(normalized_lead_details, 'external_lead_details', cur, conn, uniq)

			cur.close()
			conn.close()
		else:
			raise Exception("Account: {} not found!".format(account))

	except Exception as e:
		stack = traceback.format_exc()

		print(":::::: ERROR :::::::")
		print(e)
		print(stack)
		print(sys.exc_info())

		raise Exception(e)

if __name__ == "__main__":
	main()

