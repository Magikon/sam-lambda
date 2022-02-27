#! /usr/bin/env python

##################################################
# 8/24/16
#
# Program created by Devon K. Johnson
#
# Purpose:
#	Program creates databases from input csv data
##################################################
import os
import getopt, sys
import pdb
import re
import pandas as pd
import numpy as np
import time
from shutil import copyfile
import multiprocessing as mpp
import datetime as dt 
import monthdelta as mdd
import base64,json
import requests
import warnings
import sqlalchemy as sqal
import boto3
import psycopg2
import csv
from collections.abc import Iterable
from itertools import chain

pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

from incontact_libs import InContact 
#from mining_libs import DataPrep,ParallelPrep

DB_NAME = os.environ["DB_NAME"]
DB_USER = os.environ["DB_USER"]

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="{}-database-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 

DB_PASS = response['Parameter']['Value']
DB_HOST = os.environ["DB_ENDPOINT"]

#### Getting Redshift credentials
REDSHIFT_DB_NAME = os.environ["REDSHIFT_DB_NAME"]
REDSHIFT_DB_USER = os.environ["REDSHIFT_DB_USER"]

client = boto3.client('ssm')
response = client.get_parameter(
    Name="/{}/redshift/master-database-password".format(os.environ["ENV"]), 
    WithDecryption=True
) 

REDSHIFT_DB_PASS = response['Parameter']['Value']
REDSHIFT_DB_ENDPOINT = os.environ["REDSHIFT_ENDPOINT"]
REDSHIFT_DB_PORT = os.environ["REDSHIFT_DB_PORT"]


# Set multi processing core count
NCORE = int(mpp.cpu_count()*0.95)

###################################
# Grab Command Line Arguments
# Define current account
###################################

# read commandline arguments, first
fullCmdArguments = sys.argv
# - further arguments
argumentList = fullCmdArguments[1:]
unixOptions = "ho:v"
gnuOptions = ["account=", "report-id=", "start-date=", "end-date=", "pull-new-data"]

try:
	arguments, values = getopt.getopt(argumentList, unixOptions, gnuOptions)
except getopt.error as err:
	# output error, and return with an error code
	print (str(err))
	print ("Incorrect parameter. Usage -> incontact_pull.py (-a|--account) <account_name> (-r|--report-id) <report_id> (-sd|--start-date) <start_date> (-ed|--end-date) <end_date> (-p|--pull-new-data)")
	sys.exit(2)

ACCOUNT = None
REPORT_ID = None
DATE_START = None
DATE_END = None
PULL_NEW_DATA = False

for currentArgument, currentValue in arguments:
	if currentArgument in ("-a", "--account"):
		print (("Account: (%s)") % (currentValue))
		ACCOUNT = currentValue
	elif currentArgument in ("-r", "--report-id"):
		print (("Report ID: (%s)") % (currentValue))
		REPORT_ID = currentValue
	elif currentArgument in ("-sd", "--start-date"):
		print (("Start Date: (%s)") % (currentValue))
		DATE_START = currentValue
	elif currentArgument in ("-ed", "--end-date"):
		print (("End Date: (%s)") % (currentValue))
		DATE_END = currentValue
	elif currentArgument in ("-p", "--pull-new-data"):
		print ("Pull New Data Activated")
		PULL_NEW_DATA = True

if (ACCOUNT==None):
	print("MISSING: Account. Check parameters") 
	print ("Usage -> incontact_pull.py (-a|--account) <account_name> (-r|--report-id) <report_id> (-sd|--start-date) <start_date> (-ed|--end-date) <end_date> (-p|--pull-new-data)")
	sys.exit(2)

if (REPORT_ID==None):
	print("MISSING: Report ID. Check parameters") 
	print ("Usage -> incontact_pull.py (-a|--account) <account_name> (-r|--report-id) <report_id> (-sd|--start-date) <start_date> (-ed|--end-date) <end_date> (-p|--pull-new-data)")
	sys.exit(2)

if (DATE_START==None):
	print("MISSING: Start Date. Check parameters") 
	print ("Usage -> incontact_pull.py (-a|--account) <account_name> (-r|--report-id) <report_id> (-sd|--start-date) <start_date> (-ed|--end-date) <end_date> (-p|--pull-new-data)")
	sys.exit(2)

SAVE_AS_FILE = 'false'
INCLUDE_HEADERS = 'true'

START_YEAR  = int(DATE_START[0:4])
START_MONTH = int(DATE_START[4:6])
START_DAY   = int(DATE_START[6:8])

if (DATE_END==None):
	print("No END_DATE specified. Setting to now()")
	END_YEAR  = (dt.datetime.now() + dt.timedelta(days=1)).year
	END_MONTH = (dt.datetime.now() + dt.timedelta(days=1)).month
	END_DAY   = (dt.datetime.now() + dt.timedelta(days=1)).day
else:
	END_YEAR  = int(DATE_END[0:4])
	END_MONTH = int(DATE_END[4:6])
	END_DAY   = int(DATE_END[6:8])

API_START_DATE = "{}-{}-{}".format(str(START_YEAR),str(START_MONTH).zfill(2),str(START_DAY).zfill(2))
API_END_DATE = "{}-{}-{}".format(str(END_YEAR),str(END_MONTH).zfill(2),str(END_DAY).zfill(2))

VERSION = "17.0"
TABLENAME = 'call_log_incontact_TEST'

####################################
####################################
############ FUNCTIONS #############  
####################################
####################################

def upload_to_s3(account, csv_file):
	print("::::: Uploading CSV to S3")

	s3 = boto3.resource('s3')
	s3.meta.client.upload_file(csv_file, os.environ["BUCKET"], 'calls-tmp/{}/{}'.format(account,csv_file))

	print("::::: Done")

def insert_to_csv(records, cur, conn, account, start_date, end_date):
	print("::::: Saving to CSV")	
	try:
		if len(records) > 0:
			print("::::: INSERTING ROWS")
			insert = get_insert_statement(records, cur, conn)
			start_csv_date = start_date.strftime("%Y-%m-%d_%H:%M:%S")
			end_csv_date = end_date.strftime("%Y-%m-%d_%H:%M:%S")

			csv_file = "{}_{}_to_{}.csv".format(account, start_csv_date, end_csv_date)
			try:
				with open(csv_file, 'w') as csvfile:
					writer = csv.DictWriter(
					csvfile, 
					fieldnames=insert['csv_columns'],
					delimiter="|"
					)
					writer.writeheader()
					for data in insert['records']:
						if 'call_datetime' in data:
							data['call_datetime'] = dt.datetime.strptime(data['call_datetime'],"%m/%d/%Y %H:%M:%S")
						writer.writerow(data)
			except IOError as err:
				print("I/O error: ", err)

			print("::::: TOTAL CALLS INSERTED: {} FOR {}".format(len(insert['records']), account))
			print("::::: CSV LOCATION: {}".format(csv_file))
			return csv_file
		else:
			print("There isn't anything to insert")

	except Exception as err:
		print("----> DB error: " + str(err))
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
		conn.rollback()

def get_insert_statement(records, cur, conn):
	print("::::: Creating CSV insert statement")  

	query = "SELECT \"column\" from pg_table_def where tablename='call_logs'"
	rs_cur.execute(query)
	data = rs_cur.fetchall()

	csv_columns = []
	ignored_columns = ['id']

	for line in data:
		if line[0] not in ignored_columns:
			csv_columns.append(line[0])

	rows = []
	for i, record in enumerate(records):
		row = dict(record)
		
		date = dt.datetime.now()

		for x, key in enumerate(csv_columns):
			if key == "created_at" or key == "updated_at":
				value = "{}".format(date)
			else:
				value = row[key] if key in row else None

			row[key] = value

		rows.append(row)

	return {'records': rows, 'csv_columns': csv_columns}

def cast(value, field, datatype):
	if value != None:
		val = value
		if ("INT" in datatype):
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
				try:
					return int( float( val ) )
				except Exception as e:
					# print e
					# print val
					return -1

		elif("CHAR" in datatype):
			if (field == 'role_group'):
				val = re.sub('Team| ', '', value)
		
			return str(val)

		return val
	else:
		return value

###################################
###################################
############## Main ###############
###################################
###################################
#def velocify_daily_update():

#pap = ParallelPrep(NCORE)

##################
# Connect to Postgres
##################
#conn = psycopg2.connect('host={} dbname={} user={} password={}'.format(DB_HOST, DB_NAME, DB_USER, DB_PASS))
#cur = conn.cursor()

####################
# Connect to Redshift
####################
rs_conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(REDSHIFT_DB_NAME, REDSHIFT_DB_ENDPOINT, REDSHIFT_DB_PORT, REDSHIFT_DB_USER, REDSHIFT_DB_PASS))
rs_cur = rs_conn.cursor()


rs_cur.execute("select id from accounts where name = '{}'".format(ACCOUNT))
cur_account_id = int(rs_cur.fetchone()[0])

###########################
# Get incontact credentials 
###########################

rs_cur.execute("select call_sw_username, call_sw_password from accounts WHERE id=8")

dat = rs_cur.fetchone()
incontact_username = dat[0] 
incontact_password = dat[1]

inc = InContact(ACCOUNT,base64.b64encode(b'DataExtractionLayer@ProPair:4598179').decode('ascii'), incontact_username, incontact_password)

#########################################
# Grab Token / Check if valid
# a) if exists, load and check if valid
# b) if not exists, create new
#########################################
if os.path.exists('./token.json'):
	print('Token Exists . Use Current')
	with open('./token.json') as json_file:
		access_json = json.load(json_file)
	
else:
	print('Token Does Not Exist . Create New Token')
	r = inc.getToken()

	access_json = json.loads(r.content)
	
	with open('token.json', 'w') as outfile:
		json.dump(access_json, outfile)
	
DATE_START_dt = dt.datetime.strptime(DATE_START, '%Y%m%d')
DATE_END_dt = dt.datetime.strptime("{}{}{}".format(str(END_YEAR),str(END_MONTH).zfill(2),str(END_DAY).zfill(2)), '%Y%m%d')
cur_DATE_END_dt = DATE_END_dt
	
#########################################
# Check if data from 554 is available
#########################################
if (REPORT_ID == str(540)):
	file_554 = './incontact_{}_reportId554_{}_{}.csv.gz'.format(ACCOUNT,DATE_START,dt.datetime.strftime(DATE_END_dt,'%Y%m%d'))
	if os.path.exists(file_554):
		print('File Exists . {}'.format(file_554))
	else:
		print('File Does Not Exist . {}'.format(file_554))
		print("Before Running Report 540, Run 'python incontact_PARALLEL.py {} 554 {}'".format(ACCOUNT,DATE_START))
		sys.exit()

if PULL_NEW_DATA:	
	report_data_list = []
	while (cur_DATE_END_dt > DATE_START_dt):
		##############################
		# Check if token is valid
		##############################
		status_code = 401
		while (status_code == 401):
			BASEURL = str(access_json['resource_server_base_uri'])
			accessToken = str(access_json['access_token'])
		
			#add all necessary headers
			header_param = {'Authorization': 'bearer ' + accessToken,'content-Type': 'application/x-www-form-urlencoded','Accept': 'application/json, text/javascript, */*'}
		
			cur_DATE_START_dt = cur_DATE_END_dt - dt.timedelta(days=1)
			API_START_DATE = "{}-{}-{}".format(str(cur_DATE_START_dt.year),str(cur_DATE_START_dt.month).zfill(2),str(cur_DATE_START_dt.day).zfill(2))
			API_END_DATE = "{}-{}-{}".format(str(cur_DATE_END_dt.year),str(cur_DATE_END_dt.month).zfill(2),str(cur_DATE_END_dt.day).zfill(2))
			fileName = 'propair_reportId{}_{}_{}.csv'.format(REPORT_ID,API_START_DATE.replace('-',''),API_END_DATE.replace('-',''))
		
			############################
			# Create InContact Report
			############################
			print('Create InContact Report File . (ReportId,Start Date,End Date) . ({},{},{})'.format(REPORT_ID,API_START_DATE,API_END_DATE))
		
			# Make http post request
			https_url = "{}/services/v{}/report-jobs/datadownload/{}?startDate={} 00:00:00&endDate={} 00:15:00&saveAsFile={}&includeHeaders={}&filename={}"\
								.format(BASEURL,VERSION,REPORT_ID,API_START_DATE,API_END_DATE,SAVE_AS_FILE,INCLUDE_HEADERS,fileName)

			data_response = requests.post(https_url,headers=header_param)
		
			status_code = data_response.status_code
			print('GET DATA: HTTPS Request Status Code . {}'.format(data_response.status_code))
			if (data_response.status_code == 401):
				print('Token Expired . Create New Token')
				r = inc.getToken()
		
				access_json = json.loads(r.content)
		
				with open('token.json', 'w') as outfile:
					json.dump(access_json, outfile)
		
		if (data_response.status_code == 200):
			encoded_report_data = json.loads(data_response.content)
			report_data = base64.b64decode(encoded_report_data['file']).decode('ascii')
		
			####################
			# Find # of columns
			####################
			#report_data_df = pd.DataFrame([x.split(',') for x in report_data[:-2].split('\r\n')])
			TMP_report_data = report_data[:-2].split('\r\n')
			Ncols = len(TMP_report_data[0].split(",")) #Should always work since first row in column name
			#report_data_df = pd.DataFrame([x.split(',') if len(x.split('"""')) == 1 else str("test,"*Ncols).split(',') for x in TMP_report_data])
			
			###########################################################################################
			# Split into columns
			# 1) if len(x.split) == 1, just split
			# 2) if len(x.split) > 1, we have text potentially with commas, must treat separately
			# 	2a) Replace strings that cause extra columns ,""" and """,
			#	2b) Split by """
			#	2c) Split , in even indices / don't split odd indices
			#	2d) Stitch together with chain
			###########################################################################################
			report_data_df = pd.DataFrame([x.split(',') if len(x.split('"""')) == 1 \
											else list(chain.from_iterable(item if isinstance(item,Iterable) \
														and not isinstance(item, str) else [item] \
														for item in [t.split(",") if (iii % 2) == 0 else t \
														for iii,t in enumerate(x.replace(',""",','"""').replace(',"""','"""').replace('""",','"""').split('"""'))])) \
												for x in TMP_report_data])
	
			report_data_df.columns = report_data_df.iloc[0]
			report_data_df = report_data_df.drop(0)
			report_data_df = report_data_df.reset_index(drop=True) 
			report_data_list.append(report_data_df)
			print('(ReportId,Start Date,End Date) . ({},{},{}) . NUMBER OF RECORDS . {}'.format(REPORT_ID,API_START_DATE,API_END_DATE,np.shape(report_data_df)))
			
		cur_DATE_END_dt = cur_DATE_END_dt - dt.timedelta(days=1)
	
	#######################
	# Add all DFs together
	#######################
	output_df = pd.concat(report_data_list,ignore_index=True,sort=True)
	
	if 'DateOfCall' in output_df.columns:
		print("PRE-FILTER  . len(output_df) = {}".format(len(output_df)))
		output_df = output_df[output_df['DateOfCall'] != '']
		print("POST-FILTER . len(output_df) = {}".format(len(output_df)))

		print("Convert 'DateOfCall' to datetime")
		output_df['DateOfCall'] = pd.to_datetime(output_df['DateOfCall'],format = '%m/%d/%Y %I:%M:%S %p')
		print("COMPLETE . Convert 'DateOfCall' to datetime")
	
	########################
	# Export Data
	########################	
	out_file = './incontact_{}_reportId{}_{}_{}.csv'.format(ACCOUNT,REPORT_ID,DATE_START,dt.datetime.strftime(DATE_END_dt,'%Y%m%d'))
	
	print("Export CSV File . {}".format(out_file))
	output_df.to_csv(out_file)
	os.system('gzip -f {}'.format(out_file))
	print("Gzip CSV File . {}.gz".format(out_file))

else:
	in_file = './incontact_cardinal_reportId540_{}_{}.csv.gz'\
						.format(DATE_START, DATE_END)
	print("Import File . {}".format(in_file))
	output_df = pd.read_csv(in_file,compression='gzip')
	print("COMPLETE . Import File . {}".format(in_file))

##################
# Set account_id
##################
output_df['account_id'] = cur_account_id

if (REPORT_ID == str(540)):
	print("Import File . {}".format(file_554))
	lead_output_df = pd.read_csv(file_554,compression = 'gzip')
	lead_output_df = lead_output_df.rename(columns={'ContactID':'Contact_ID'})
	output_df['Contact_ID'] = output_df['Contact_ID'].astype('int64')
	lead_output_df['Contact_ID'] = lead_output_df['Contact_ID'].astype('int64')
	print("PRE-MERGE  . len(output_df) = {}".format(len(output_df)))
	output_df = pd.merge(output_df,lead_output_df,'left',left_on='Contact_ID',right_on='Contact_ID')

	print("POST-MERGE . len(output_df) = {}".format(len(output_df)))

	#######################
	# Agent_No correction
	#######################
	output_df['Agent_No_x'] = [y if (x == 0) & (pd.isnull(y) == False) else x for x,y in zip(output_df['Agent_No_x'],output_df['Agent_No_y'])]
	output_df = output_df.drop('Agent_No_y',1)
	output_df = output_df.rename(columns={'Agent_No_x':'Agent_No'})

	#######################
	# Field Mapping
	#######################
	print("Normalizing calls..")
	sql = """
		SELECT propair_field, customer_field_name, customer_split, datatype FROM global_attribute_lookup
		WHERE account_system='InContact' AND table_name='call_logs';
	"""
	rs_cur.execute(sql)
	data = rs_cur.fetchall()

	#######################
	# Drop Duplicates
	#######################
	map_dict = dict(zip([x[0] for x in data],[x[1] for x in data]))
	map_dict['account_id'] = 'account_id'
	unique_col = ['account_id','contact_id','call_id']
	print("UNIQUE_COLS . {}".format(unique_col))
	print("PRE-DROP DUPLICATES   . len(output_df) = {}".format(len(output_df)))
	output_df = output_df.drop_duplicates([map_dict[x] for x in unique_col]).reset_index()
	print("POST-DROP DUPLICATES  . len(output_df) = {}".format(len(output_df)))

	attribute_map = []
	for line in data:
		attribute_map.append( { 'propair': line[0], 'customer': line[1], 'split': line[2], 'datatype': line[3] } )

	normalized_calls = []
	calls = json.loads(output_df.to_json(orient='records'))
	missing_ids = 0
	for call in calls:
		normalized_call = {}
		joined = {}

		for attr in attribute_map:
			
			if(attr['split'] != None):
				split_data = attr['customer'].split(':')
				field = split_data[0]
				position = int(split_data[1])

				if(field in call):
					values = call[field].split(attr['split'])
					if (len(values) > position):
						normalized_call[attr['propair']] = cast(values[position], attr['propair'], attr['datatype'])

			elif(':' in attr['propair']):
				split_data = attr['propair'].split(':')
				field = split_data[0]
				position = int(split_data[1])
				customer_field = attr['customer']

				if(field not in joined.keys()):
					joined[field] = []
				for _ in range(position-len(joined[field])+1):
					joined[field].append(None)

				joined[field][position] = cast(call[customer_field], field,  attr['datatype'])
			elif(attr['customer'] in call):
				normalized_call[attr['propair']] = cast(call[attr['customer']], attr['propair'],  attr['datatype'])

		for k, j in joined.items():
			normalized_call[k] = ' '.join(j)

		normalized_call['account_id'] = call['account_id']
		normalized_calls.append(normalized_call)
	
	csv = insert_to_csv(normalized_calls, rs_cur, rs_conn, ACCOUNT, DATE_START_dt, DATE_END_dt)

	upload_to_s3(ACCOUNT, csv)
	rs_cur.close()
	rs_conn.close()

	#renamed = {}
	#splits = {}
	#joins = {}
	#for attr in attribute_map:
	#	if(attr['split'] != None):
	#		split_data = attr['customer'].split(':')
	#		field = split_data[0]
	#		position = int(split_data[1])
#
	#		if(field not in splits.keys()):
	#			splits[field] = {'delimiter': attr['split'], 'fields': []}
	#		for _ in range(position-len(splits[field]['fields'])+1):
	#			splits[field]['fields'].append(None)
#
	#		splits[field]['fields'][position] = attr['propair']
#
	#	elif(':' in attr['propair']):
	#		split_data = attr['propair'].split(':')
	#		field = split_data[0]
	#		position = int(split_data[1])
	#		customer_field = attr['customer']
#
	#		if(field not in joins.keys()):
	#			joins[field] = []
	#		for _ in range(position-len(joins[field])+1):
	#			joins[field].append(None)
#
	#		joins[field][position] = customer_field
	#	else:
	#		renamed[attr['customer']] = attr['propair']
#
	##Split Columns
	#for key, row in splits.items():
	#	split_len = len(row['fields'])
	#	output_df[row['fields']] = output_df[key].apply( 
	#	lambda x: pd.Series(
	#			str(x).split(row['delimiter'])[0:split_len] if len(str(x).split(row['delimiter'])) >= split_len else []
	#		)
	#	) 
	#	output_df.drop(columns=key)
#
	##Join Columns
	#for key, row in joins.items():
	#	output_df[key] = output_df[row].apply(lambda x: ' '.join(x.astype(str)), axis = 1)
#
#
	##Rename Columns
	#output_df.rename(columns=renamed)
	#
#
	#		
	#schema = {'account_id':sqal.types.SmallInteger,
	#		  'Agent_No':sqal.types.Integer,
	#		  'InQueue':sqal.types.Integer,
	#		  'PreQueue':sqal.types.Integer,
	#		  'PostQueue':sqal.types.Integer,
	#		  'RoutingTime':sqal.types.Integer,
	#		  'HoldTime':sqal.types.Integer,
	#		  'Abandon_Time':sqal.types.Integer,
	#		  'Total_Time_Plus_Disposition':sqal.types.Integer,
	#		  'ACW_Time':sqal.types.Integer,
	#		  'ProspectiveContactSourceID':sqal.types.Integer,
	#		  'CallDuration':sqal.types.Integer,
	#		  'ExternalID':sqal.types.BigInteger,
	#		  'Contact_ID':sqal.types.BigInteger,
	#		  'Master_Contact_ID':sqal.types.BigInteger,
	#		  'Contact_Code':sqal.types.BigInteger,
	#		  'Disp_Code':sqal.types.BigInteger,
	#		  'DateOfCall':sqal.types.DateTime(),
	#		  'StartDate':sqal.types.Date,
	#		  'AgentDisposition':sqal.types.Integer,
	#		  'AgentDispositionDescription':sqal.types.Text,
	#		  'SystemClassification':sqal.types.SmallInteger,
	#		  'SystemClassificationName':sqal.types.Text,
	#		  'Skill':sqal.types.Text,
	#		  'Skill_Name':sqal.types.Text,
	#		  'SourceName':sqal.types.Text,
	#		  'Disp_Comments':sqal.types.Text,
	#		  'Disp_Name':sqal.types.Text,
	#		  'Agent_Name':sqal.types.Text,
	#		  'Campaign_Name':sqal.types.Text,
	#		  'Team_Name':sqal.types.Text,
	#		  'ProspectiveContactDestinationDesc':sqal.types.Text,
	#		}
#
	#
#
	#with warnings.catch_warnings():
	#	warnings.simplefilter("ignore", category=sqal.exc.SAWarning)
	#	output_df.loc[:9,:].to_sql(
	#		TABLENAME,
	#		engine,
	#		if_exists="replace",
	#		index=False,
	#		chunksize=10000,
	#		dtype = schema 
	#	)
	#	Ncase,start_idx,end_idx = pap.list_prep(output_df)
	#	print(Ncase)
	#	print('Upload SQL data in parallel . NCORE = {:3d}'.format(len(start_idx)))
#
	#	schema = None
	#	p = mpp.Pool(len(start_idx))
	#	p.map(eda_insert_query,zip(Ncase,[TABLENAME]*len(Ncase),[schema]*len(Ncase),[output_df.loc[x1:x2-1,:] if x1 != 0 else output_df.loc[10:x2-1,:] for x1,x2 in zip(start_idx,end_idx)]))
	#	p.close()
#
	#	print('Loaded {} rows of data'.format(len(output_df)))
#
	#	lead = Lead('propair_prod')
	#	eda_index_list = ['account_id','ExternalID','Contact_ID','Master_Contact_ID','Agent_No']
	#	lead.INDEX_mysql_DB(con,TABLENAME,eda_index_list)
#
	#	con.close()

##
####return velo,event_update_df,lead_df,lead_data,lead_list,lead_attribute_df,map_dict
####
####	
####if __name__ == "__main__":
####velo,event_update_df,lead_df,lead_data,lead_list,lead_attribute_df,map_dict = velocify_daily_update()
####
####lead_df.to_csv('/home/djohnson/tableau-extracts/test_api.csv')
####event_update_df.to_csv('/home/djohnson/tableau-extracts/event_api.csv')
###
