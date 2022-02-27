import sys
import os
import traceback
import requests
import xml.etree.ElementTree as ET
import psycopg2
import datetime as dt
import re
import boto3
import pytz
import yaml 
import json
import inspect
import pandas as pd
import xmltodict

from time import sleep
from json import dumps, loads
from kafka import KafkaProducer
from generate_calls import generate
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from incontact_libs import pull_report
from pplibs.customErrors import ProPairError, report_to_rollbar

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

#### Getting Kafka credentials
response = client.get_parameter(
	Name="/{}/kafka-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 
k_pass = response['Parameter']['Value']


config = yaml.load( open('./config/config.yml'), Loader=yaml.FullLoader)
config = config[os.environ['ENV']]

host= config["host"]

velocify_get_report_url = 'https://service.leads360.com/ClientService.asmx/GetCallHistoryReport'

def daterange(start_date, end_date):
	for n in range(int ((end_date - start_date).days)):
		yield start_date + dt.timedelta(n)

def cast(value, field, datatype):
	if value != None:
		val = value
		if ("INT" in datatype):
			if isinstance(val, str):
				if(str(val).lower() == 'true'):
					return 1
				elif(str(val).lower() == 'false'):
					return 0
				elif(field in ['wait_time', 'call_duration'] and len(str(val).split(':'))==2):
					dm, ds = val.split(':')
					val = int(dm) * 60 + int(ds)
					return val
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

def get_record_dict( child, account_id, mapping):


	if (isinstance(child, dict)):
		call_record = child
	else:
		call_record = child.attrib

	normalized_call = {}
	joined = {}

	for attr in mapping:
		
		if(attr['split'] != None):
			split_data = attr['customer'].split(':')
			field = split_data[0]
			position = int(split_data[1])

			if(field in call_record):
				values = call_record[field].split(attr['split'])
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

			joined[field][position] = cast(call_record[customer_field], field,  attr['datatype'])
		elif(attr['customer'] in call_record):
			normalized_call[attr['propair']] = cast(call_record[attr['customer']], attr['propair'],  attr['datatype'])

	for k, j in joined.items():
		normalized_call[k] = ' '.join(j)

	normalized_call['account_id'] = account_id
	
	return normalized_call

def on_send_success(record_metadata):
	pass
	#secure_log('{0} {1}  {2}'.format(record_metadata.topic,record_metadata.partition, record_metadata.offset) )

def on_send_error(excp):
	log.error('Something went wrong', exc_info=excp)
	py_err = None
	try:
		if isinstance(excp, str):
			py_err = ProPairError(excp, "Calls_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
		else:
			py_err = ProPairError(excp.message, "Calls_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())

	except Exception as e:
		secure_log(e)
		py_err = ProPairError(str(excp), "Calls_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
	py_err.account = ACCOUNT if ACCOUNT is not None and ACCOUNT != "" else "NO_ACCOUNT"
	report_to_rollbar(rollbar_topic, py_err)


def callparseURL(url, payload):
	try:
		r = requests.post(url,data=payload)
	except Exception as e:
		try:
			secure_log('RETRY 1 . {} . requests.post failure {}'.format(self.API_DATE_START,e))
			r = requests.post(url,data=payload)
		except:
			try:
				secure_log('RETRY 2 . {} . requests.post failure {}'.format(self.API_DATE_START,e))
				r = requests.post(url,data=payload)
			except:
				secure_log('{} . requests.post failure {}'.format(self.API_DATE_START,e))
				r = None
	try:
		output = xmltodict.parse(r.content)
	except Exception as e:
		try:
			secure_log('{} . xmltodict failure . {}'.format(self.API_DATE_START,e))
			XMLdecode = r.content.decode('utf-8')
			#XMLcorrected = r.content.encode('utf-8') 
			#XMLcorrected = re.sub('&.+[0-9]+;', '', XMLdecode)
			XMLcorrected = XMLdecode.replace('&','')
			output = xmltodict.parse(XMLcorrected)
			secure_log('{} . xmltodict success . {} . ERROR CORRECTED'.format(self.API_DATE_START,e))
		except Exception as e:
			secure_log('{} . 2nd xmltodict failure . {}'.format(self.API_DATE_START,e))

	return output,r

def getCallHistoryReport(start_date, end_date, account_meta):
	url = "https://service.leads360.com/ClientService.asmx/GetCallHistoryReport"
	payload = {
		'startDate' : start_date, 
		'endDate' : end_date,
		'username' : account_meta['velocify_username'], 
		'password' : account_meta['velocify_password']
		}

	output,r = callparseURL(url, payload)
	if ('Calls' in output.keys()):
		if (pd.isnull(output['Calls']) == False):
			try:
				output_df = pd.DataFrame(output['Calls']['Call'])
			except:
				output_df = pd.DataFrame(output['Calls']['Call'],index=[0])
			output_df.columns = [x.replace('@','') for x in output_df.columns]
		else:
			output_df = pd.DataFrame()
	else:
		output_df = pd.DataFrame()
	return output_df,r

def pull_logs(date_start, date_end, account_meta):

	cur_datetime = dt.datetime.now()
	if (date_end > cur_datetime):
		date_end = cur_datetime

	secure_log('::::: Date Start, Date End . {},{}'.format(date_start,date_end) )

	call_log_list = []
	
	#variables
	Nhour_delta = 6
	Ncase = 0
	write_freq = 50
	deltaT = 0

	d1 = date_start
	d2 = d1 + dt.timedelta(hours = Nhour_delta)
	max_call_time = dt.datetime(1900,1,1,0,0,0)

	
	grab_data = True
	while(grab_data):
		Ncase += 1
		call_log_TMP_df,r = getCallHistoryReport(d1,d2, account_meta)

		call_log_list.append(call_log_TMP_df)

		secure_log('::::: Date Start, Date End . {},{} . Nrecord = {}'.format(d1,d2,len(call_log_TMP_df)) )

		max_call_time_LAST = max_call_time
		
		if (len(call_log_TMP_df) > 0):
			call_log_TMP_df['CallTime'] = pd.to_datetime(call_log_TMP_df['CallTime'])
			max_call_time = max(call_log_TMP_df['CallTime']) - dt.timedelta(seconds = 100)
		else:
			max_call_time = d1 + dt.timedelta(hours = Nhour_delta) 

		d1 = max_call_time
		if (d1 < date_end):
			d2 = d1 + dt.timedelta(hours = Nhour_delta)
			# Keep increasing time until it finds new calls
			if (max_call_time == max_call_time_LAST):
				d2 = d2 + dt.timedelta(hours = Nhour_delta * hour_mult)
				hour_mult = hour_mult + 1
			else:
				hour_mult = 1
			if (d2 > cur_datetime):
				grab_data = False 
		else:
			grab_data = False
			
	####################
	# Create dataframe 
	####################
	call_log_df = pd.concat(call_log_list,ignore_index=True)
	##############################
	# Remove Duplicates and Sort
	##############################
	if (len(call_log_df) > 1):
		call_log_df = call_log_df.drop_duplicates('Id').sort_values('Id').reset_index(drop=True)

	secure_log('::::: Complete. Date Start, Date End . {},{} . Nrecords = {}'.format(date_start,date_end,len(call_log_df)) )
	return json.loads(call_log_df.to_json(orient='records', date_format='iso'))

def update(event, context):

	try: 
		if ('Records' in event):
			account_meta = json.loads(event['Records'][0]['Sns']['Message'])		
		else:
			account_meta = event

		account_id		   = account_meta['account_id']
		account			   = account_meta['account']

		start_dt           = dt.datetime.strptime(account_meta['start_date'],"%Y-%m-%d %H:%M:%S") if 'start_date' in account_meta else None
		end_dt 			   = dt.datetime.strptime(account_meta['end_date'],"%Y-%m-%d %H:%M:%S") if 'end_date' in account_meta else dt.datetime.now()
		records			   = []
	
		account_config = account_meta['account_config'] if 'account_config' in account_meta else get_account_config(account_id) 
		secure_log("::::: Initializing Data Architecture Calls", configuration=account_config)

		try:
			config_variables = account_config['calls_producer']['variables']
		except KeyError:
			raise Exception("Unable to find producer configuration in DynamoDB: <calls_producer.variables>")


		calls_sw = config_variables['calls_software'].lower() if 'calls_software' in config_variables else None

		accepted_sw = ['velocify', 'incontact']
		if(calls_sw is None or calls_sw not in accepted_sw):
			raise Exception("Call Software not recognized: {}".format(calls_sw))

		secure_log("::::: Call Software: {}".format(calls_sw.capitalize()))
		# fetching global_attribute
		conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()

		query = "SELECT propair_field, customer_field_name, customer_original_field_name, datatype, customer_split FROM global_attribute_lookup\
		WHERE account_id = '{0}' AND lower(account_system)='{1}' AND table_name='call_logs' AND ( customer_field_name IS NOT NULL OR customer_original_field_name IS NOT NULL);".format(account_id, calls_sw.lower())
		cur.execute(query)
		data = cur.fetchall()

		call_map = []
		
		for line in data:
			call_map.append( { 'propair': line[0], 'customer':line[1], 'original':line[2], 'datatype': line[3], 'split': line[4] } )

		if (os.environ['ENV'] == 'staging'):
			data = generate(calls_sw, cur)
		
			secure_log("::::: Mapping Records")
			for child in data:
				record = get_record_dict( child, account_id, call_map )
				records.append( record )
		
		elif(calls_sw=='velocify'):
			conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
			cur = conn.cursor()


			if ('velocify_username' not in account_meta):
				secure_log("::::: Fetching account meta data")
				query = "SELECT velocify_username, velocify_password from accounts WHERE id={}".format(account_id)
				cur.execute(query)
				d = cur.fetchone()

				account_meta['velocify_username'] = d[0]
				account_meta['velocify_password'] = d[1] 


			if (start_dt == None):
				#Grab last event timestamp
				query = "SELECT max(call_datetime) as call_timestamp FROM call_logs WHERE account_id={};".format(account_id)
				cur.execute(query)
				data = cur.fetchall()
				cur.close()
				conn.close()

				if( len(data) > 0 and len( data[0] ) > 0 and data[0][0] != None ): 
					start_dt = data[0][0]
				else:
					start_dt = dt.datetime(2020, 8, 1, 0, 0, 0, 0)

			result = pull_logs(start_dt, end_dt, account_meta)

			for child in result:
				record = get_record_dict( child, account_id, call_map  )
				records.append( record )

		elif(calls_sw=='incontact'):
			cur.execute("select call_sw_username, call_sw_password from accounts WHERE id={}".format(account_id))
			dat = cur.fetchone()
			incontact_username = dat[0] 
			incontact_password = dat[1]
			try_number = 0
			while try_number < 3:
				try:
					report_540_df = pull_report(540, account_config, config_variables, incontact_username, incontact_password)
					report_554_df = pull_report(554, account_config, config_variables, incontact_username, incontact_password)

					report_554_df = report_554_df.rename(columns={'ContactID':'Contact_ID'})
					
					report_554_df['Contact_ID'] = report_554_df['Contact_ID'].astype('int64')
					report_540_df['Contact_ID'] = report_540_df['Contact_ID'].astype('int64')

					secure_log("::::: PRE-MERGE  . len(Report 540) = {}".format(len(report_540_df)))
					output_df = pd.merge(report_540_df,report_554_df,'left',left_on='Contact_ID',right_on='Contact_ID')
					secure_log("::::: POST-MERGE . len(Output) = {}".format(len(output_df)))
					break
				except Exception as err:
					secure_log(err)
					try_number += 1
					if try_number >= 3:
						raise err
					secure_log("Trying again...")
			
			
			

			#######################
			# Agent_No correction
			#######################
			output_df['Agent_No_x'] = [y if (x == 0) & (pd.isnull(y) == False) else x for x,y in zip(output_df['Agent_No_x'],output_df['Agent_No_y'])]
			output_df = output_df.drop('Agent_No_y',1)
			output_df = output_df.rename(columns={'Agent_No_x':'Agent_No'})

			calls = json.loads(output_df.to_json(orient='records'))

			secure_log("::::: Mapping Records")
			for child in calls:
				record = get_record_dict( child, account_id, call_map )
				records.append( record )

		
		secure_log("Total of {} records processed.".format(len(records)))
		
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

		for i, e in enumerate(records):
			k = bytes(int(e['account_id']))
			p = producer.send('call_logs', key=k, value=e).\
				add_callback(on_send_success).\
				add_errback(on_send_error)
		
		secure_log(":::: {} Records sent to stream".format(len(records)))

		# block until all sync messages are sent
		producer.flush()
		producer.close()
		cur.close()
		conn.close()

	except Exception as err:
		print("::::: ERROR :::::")
		print(err)
		py_err = ProPairError(err, "Calls_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())


		try:
			account_meta = json.loads(event['Records'][0]['Sns']['Message'])
			account = account_meta['account']
			py_err.account = account
			report_to_rollbar(rollbar_topic, py_err)
			
		except Exception as err2:
			print("::::: ROLLBAR ERROR :::::")
			print(err2)
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)
