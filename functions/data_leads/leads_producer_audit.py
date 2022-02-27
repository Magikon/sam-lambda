import sys
import os
import math
import traceback
import requests
import xml.etree.ElementTree as ET
import psycopg2
import datetime
import re
import boto3
import pytz
import xmltodict
import yaml 
import json as js
import pandas as pd
import numpy as np

from time import sleep
from kafka import KafkaProducer
from ricochet import Ricochet

from db_utils import get_unique_agents
from utils import get_unique_agents_ricochet, get_column_data
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.customErrors import ProPairError, report_to_rollbar
from pplibs.db_utils import build_record_values, run_transaction

sns = boto3.client('sns')

rollbar_topic = os.environ["TOPIC_ROLLBAR"]

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 
db_pass = response['Parameter']['Value']

##### Redshift env variables 
db_host = os.environ["REDSHIFT_ENDPOINT"]
db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = 5439

def get_leads(account_meta):

	def fetch_leads(account_meta):

		API_START_DATE = '01/01/2015'
		url = "https://service.leads360.com/ClientService.asmx/GetLeadIds"

		payload = {'username' : account_meta['velocify_username'], 'password' : account_meta['velocify_password'], 'from': API_START_DATE,'to': datetime.datetime.now().strftime('%m/%d/%y')}

		try:
			r = requests.post(url,data=payload)
		except Exception as e:
			secure_log('{} . requests.post failure {}'.format(API_DATE_START,e))
			try:
				secure_log('RETRY 1 . {} . requests.post failure {}'.format(API_DATE_START,e))
				r = requests.post(url,data=xml,headers=headers)
			except:
				try:
					secure_log('RETRY 2 . {} . requests.post failure {}'.format(API_DATE_START,e))
					r = requests.post(url,data=xml,headers=headers)
				except:
					secure_log('{} . requests.post failure {}'.format(API_DATE_START,e))
					r = None

		try:
			lead_list = xmltodict.parse(r.content)

			### Convert output ###  
			lead_list = js.loads(js.dumps(lead_list["Leads"]["Lead"]))
			lead_list = [int(x['@Id']) for x in lead_list]

		except Exception as e:
			secure_log('{} . xmltodict failure {}'.format("01/01/2015",e))
			lead_list = {}


		return lead_list

	#start_date = datetime.datetime.strptime(account_meta['start_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M %p")
	#end_date = datetime.datetime.strptime(account_meta['end_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M %p")

	secure_log("::::: Fetching LMS Lead List")
	lead_list = fetch_leads(account_meta)

	return lead_list

def build_insert_query(table, records, column_data):
	sql = 'INSERT INTO {0}({1})'.format(table, ', '.join(column_data.keys()))
	values = "VALUES "

	for i, record in enumerate(records):
		record_values = build_record_values(record, column_data)

		values += "({})".format(', '.join(record_values))
		if i < len(records) - 1:
			values += ", "

	sql += values + ';'

	return sql


def get_table_constraints(table, cur):
	sql = f"select column_name from view_constraints where table_name='{table}' and constraint_type='UNIQUE';"

	cur.execute(sql)
	
	result = list(set([x[0] for x in cur.fetchall()]))
	
	if len(result) > 0:
		return result
	else:
		raise Exception(f"No table constraints found for {table}")
		

def build_update_transaction(table, records, column_data, unique_cols):
	stage_name = f'temp_{table}'

	# Query start
	sql = f"""
		begin read write;
		create temp table {stage_name}
			(like {table});
		"""

	sql += build_insert_query(stage_name, records, column_data)

	update_query = f"update {table} set "
	update_values = []

	for col in column_data:
		if col not in unique_cols and col != 'created_at':
			update_values.append("{0} = {1}.{0}".format(col, stage_name))

	update_query += "{}".format(', '.join(update_values))
	update_query += " from {} where ".format(stage_name)

	for i, item in enumerate(unique_cols):
		update_query += "{2}.{0} = {1}.{0}".format(item, stage_name, table)
		if i < len(unique_cols) - 1:
			update_query += " and "
		else:
			update_query += ";"

	sql += update_query
	sql += f"""
		end transaction;\
		drop table {stage_name};
		"""

	return sql

def update(event, context):
	try:
		if ('Records' in event):
			account_meta = js.loads(event['Records'][0]['Sns']['Message'])		
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

		if (leads_software != 'velocify'):
			raise Exception("Lead Software: {} not supported yet!".format(leads_software))

		secure_log("::::: ACCOUNT META", account_meta)
		secure_log("::::: LEADS SOFTWARE: ", leads_software)
		
		lms_lead_list = None
		success = False
		counter = 0
		while(success == False and counter < 3):
			lms_lead_list = get_leads(account_meta)
				
			if(lms_lead_list != None and len(lms_lead_list) > 0):
				secure_log("::::: Succesfully retrieved LMS leads")
				success = True
			else:
				counter += 1
				secure_log("::::: Failed to retrieve LMS leads. Trying again.. Count: {}".format(counter))

	
		if(success == True):

			secure_log("::::: Fetching Redshift Lead List")

			env = os.environ["ENV"]

			if env == 'production':
				sq = 'SELECT account_id,account_lead_id from external_leads where account_id = {} and account_lead_id IS NOT NULL and idup = 0'.format(account_id)
			elif env == 'staging':
				sq = "SELECT account_id,account_lead_id from external_leads where account_lead_id IS NOT NULL and idup = 0".format(account_id)

			cur.execute(sq)
			rs_df =  pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])

			rs_lead_list = list(rs_df['account_lead_id'])

			secure_log("::::: Total Leads . LMS,Redshift . {},{}".format(len(lms_lead_list),len(rs_lead_list)))

			secure_log("::::: Find Duplicates in Table: external_leads")
			dup_list = list(sorted(set(rs_lead_list) - set(lms_lead_list)))
			secure_log("::::: Duplicate Lead Removal Count . {}".format(len(dup_list)))

			if (len(dup_list) > 0):
				column_data = get_column_data('external_leads', cur)
				constraints = get_table_constraints('external_leads', cur)
				dup_df = rs_df[rs_df['account_lead_id'].isin(dup_list)].reset_index()
				dup_df['idup'] = 1
				dup_df['idup_date'] = datetime.datetime.now()

				chunk_size = 10000

				if (len(dup_df) > chunk_size):
					secure_log("::::: Size over limit; spliting into chunks of {}".format(chunk_size))
					split_df = np.array_split(dup_df, math.ceil(len(dup_df)/chunk_size))
					
					for df in split_df:
						records = js.loads(df.to_json(orient='records', date_format='iso'))
						secure_log("::::: Updating {} records".format(len(df)))
						sq = build_update_transaction('external_leads', records, {x['name']: x for x in column_data.values() if x['name'] in df.columns}, constraints)
						run_transaction(cur, conn, sq)
				else:
					records = js.loads(dup_df.to_json(orient='records', date_format='iso'))

					secure_log("::::: Updating {} records".format(len(dup_df)))
					sq = build_update_transaction('external_leads', records, {x['name']: x for x in column_data.values() if x['name'] in dup_df.columns}, constraints)
					run_transaction(cur, conn, sq)

					

			else:
				secure_log("::::: No Duplicates to Update")

			secure_log("::::: Find Missing Leads in Table: external_leads")
			missing_list = list(sorted(set(lms_lead_list) - set(rs_lead_list)))
			secure_log("::::: Missing Leads count . {}".format(len(missing_list)))

			if (len(missing_list) > 0):
				missing_df = pd.DataFrame(missing_list, columns=['account_lead_id'])
				missing_df['account_id'] = account_id

				# TODO: Make this dynamic once more systems are added
				missing_df['account_system'] = 'velocify'		

				secure_log("::::: Deleting prior Missing Leads")
				# Delete prior missing_leads
				sq = 'delete from missing_leads where account_id={};'.format(account_id)
				cur.execute(sq)
				conn.commit()

				secure_log("::::: Inserting current Missing Leads")

				chunk_size = 140000
				column_data = get_column_data('missing_leads', cur)

				if (len(missing_df) > chunk_size):
					secure_log("::::: Size over limit; spliting into chunks of {}".format(chunk_size))
					split_df = np.array_split(missing_df, math.ceil(len(missing_df)/chunk_size))
					for df in split_df:
						records = js.loads(df.to_json(orient='records', date_format='iso'))

						# Insert current missing_leads
						sq = build_insert_query('missing_leads', records, column_data)
						run_transaction(cur, conn, sq)
				else:
					records = js.loads(missing_df.to_json(orient='records', date_format='iso'))

					# Insert current missing_leads
					sq = build_insert_query('missing_leads', records, column_data)
					run_transaction(cur, conn, sq)
			else:
				secure_log("::::: No Leads missing!")

			cur.close()
			conn.close()
			secure_log("::::: Done Processing Leads")
	   
		else:
			raise Exception("Failed to Pull Leads From LMS")

		cur.close()
		conn.close()
		secure_log("::::: All Done.")
	except Exception as err:
		secure_log("::::: ERROR ", err)
		py_err = ProPairError(err, "Leads_Audit_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		try:
			account_meta = js.loads(event['Records'][0]['Sns']['Message'])
			account = account_meta['account']
			py_err.account = account
			report_to_rollbar(rollbar_topic, py_err)
			conn.rollback()
		except Exception as err:
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)
			conn.rollback()
		

