import sys
import os
import traceback
import gzip
import yaml
from json import dumps
from io import BytesIO, TextIOWrapper
from kafka import KafkaProducer

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
import csv
import pandas as pd

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
pd.set_option('display.max_rows',400)

from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log

tmp_folder = '/tmp'

##### Redshift env variables 
db_host = os.environ["REDSHIFT_ENDPOINT"]
db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = 5439

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
config = yaml.load( open('./config/config.yml'), Loader=yaml.FullLoader)
config = config[os.environ['ENV']]

def update(event, context):
	try:
		
		secure_log(event['Records'][0]['s3'])
		key = event['Records'][0]['s3']['object']['key']
		key = re.sub('\+', ' ', key)
		bucket = event['Records'][0]['s3']['bucket']['name']
		file_name_pieces = key.split('/')
		account_name = file_name_pieces[len(file_name_pieces)-3]
		account_id = None
		file_name = file_name_pieces[len(file_name_pieces) - 1]
		secure_log("::::: File Name: {}".format(file_name))

		conn = psycopg2.connect('host={} dbname={} user={} password={} port={}'.format(db_host, db_name, db_user, db_pass, db_port))
		cur = conn.cursor()

		secure_log("::::: Fetching Account Info")
		sq = "SELECT id FROM accounts WHERE name = '{}';".format(account_name.lower())
		cur.execute(sq)
		data = cur.fetchall()
		for account_row in data:
			account_id = account_row[0]

		account_config = get_account_config(account_id)

		secure_log("::::: Fetching Global Attribute Mapping Info")
		attribute_map = []
		try:
			query = "SELECT propair_field, customer_field_name, customer_original_field_name, customer_split, datatype FROM global_attribute_lookup L INNER JOIN accounts A on L.account_id = A.id WHERE A.name = '{0}' AND (account_system = 'Encompass' AND include_variable = 1) OR (table_name ='all');".format(account_name.lower())
			cur.execute(query)
			data = cur.fetchall()
			for line in data:
				attribute_map.append( { 'propair': line[0], 'customer': line[1], 'original': line[2], 'split': line[3], 'datatype': line[4] } )
		except Exception as err:
			stack = traceback.format_exc()
			report_to_rollbar(err, str(stack), str(sys.exc_info()), account_name)
			secure_log("I am unable to connect to the database")
			
		if (len(attribute_map) == 0):
			raise Exception("No data in global attribute map!")

		secure_log("::::: Downloading new file from S3")

		s3 = boto3.client('s3')
		local_file = '{0}/{1}'.format(tmp_folder, file_name)
		s3.download_file(bucket, key, local_file)

		secure_log("::::: Parsing CSV")
		los_df = pd.read_csv(local_file,encoding = "ISO-8859-1")

		secure_log("::::: Normalizing Records")

		for attribute in attribute_map:
			if str(attribute['original']).strip() in los_df.columns:
				los_df = los_df.rename(columns={attribute['original']: attribute['propair']})
				los_df[attribute['propair']] = los_df[attribute['propair']].apply(lambda x: sanitize_and_cast(x, attribute['datatype']))
				
				if 'INT' in attribute['datatype']:
					## Set to float / make < sys.maxsize to avoid 'Python int too large to convert to C long' error 
					los_df = los_df.astype({attribute['propair']: 'float'},errors='ignore')
					los_df[attribute['propair']] = [None if pd.isnull(x) == True else sys.maxsize-1 if x >= sys.maxsize else int(x) for x in los_df[attribute['propair']]]
					#los_df = los_df.astype({attribute['propair']: 'Int64'},errors='ignore')
				elif 'DATE' in attribute['datatype']:
					#los_df = los_df.astype({attribute['propair']: 'datetime64'},errors='ignore')
					los_df[attribute['propair']] = pd.to_datetime(los_df[attribute['propair']],errors='ignore')

			elif str(attribute['customer']).strip() in los_df.columns:
				los_df = los_df.rename(columns={attribute['customer']: attribute['propair']}) 
				los_df[attribute['propair']] = los_df[attribute['propair']].apply(lambda x: sanitize_and_cast(x, attribute['datatype']))
			   
				if 'INT' in attribute['datatype']:
					## Set to float / make < sys.maxsize to avoid 'Python int too large to convert to C long' error 
					los_df = los_df.astype({attribute['propair']: 'float'},errors='ignore')
					los_df[attribute['propair']] = [None if pd.isnull(x) == True else sys.maxsize-1 if x >= sys.maxsize else int(x) for x in los_df[attribute['propair']]]
					#los_df = los_df.astype({attribute['propair']: 'Int64'},errors='ignore')

		initial_len = len(los_df)
		los_df = los_df.dropna(subset=['account_loan_id'])
		secure_log("::::: Dropped {} null rows".format(len(los_df) - initial_len))

		####################################
		# Remove non email addresses
		####################################
		
		if ('borrower_email' in list(los_df.columns)):
			secure_log("::::: Removing bad Borrower Emails")
			los_df.at[(los_df['borrower_email'].str.contains('.+@.+\.\w+', flags=re.IGNORECASE, regex=True) == False), 'borrower_email'] = None
		if ('coborrower_email' in list(los_df.columns)):
			secure_log("::::: Removing bad Co-Borrower Emails")
			los_df.at[(los_df['coborrower_email'].str.contains('.+@.+\.\w+', flags=re.IGNORECASE, regex=True) == False), 'coborrower_email'] = None

		####################################
		# Remove dups
		####################################
		use_loan_id = account_config['los']['use_loan_id'] if 'use_loan_id' in account_config['los'] else True

		if (use_loan_id):
			los_df = remove_duplicates(los_df)

		####################################
		# Set account_id
		####################################
		los_df['account_id'] = account_id

		####################################
		# Upload to s3
		####################################
		gz_buffer = BytesIO()

		with gzip.GzipFile(mode='w', fileobj=gz_buffer) as gz_file:
			los_df.to_csv(TextIOWrapper(gz_file, 'utf8'), index=False)

		s3_resource = boto3.resource('s3')
		file_path = 'los/download/{}_los-{}.csv.gz'.format(account_name.lower(),datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S'))
		s3_object = s3_resource.Object(os.environ['BUCKET'], file_path)
		s3_object.put(Body=gz_buffer.getvalue())

		secure_log("::::: Succesfully Uploaded file to s3. Bucket: {}, Path: {}".format(os.environ["BUCKET"],file_path))

		producer = KafkaProducer(
					bootstrap_servers=config["host"],
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

		secure_log("::::: Producer created :::::")

		message_meta = {
			"account_id": account_id,
			"account_name": account_name,
			"bucket": os.environ["BUCKET"],
			"file_path": file_path
		}

		k = bytes(int(account_id))
		p = producer.send('los', key=k, value=message_meta)
		
		secure_log(":::: File Path sent to stream")
		
		producer.flush()
		producer.close()
		cur.close()
		conn.close()
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), account_name)

def sanitize_and_cast(val, to_data_type):
	if(val == None):
		return None
	if( "CHAR" in to_data_type ):
		#TODO: Sanitize before casting
		return str(val).replace("$$", r"$")
	elif( "INT" in to_data_type or "FLOAT" in to_data_type):
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
						# secure_log(e)
						# secure_log(val)
						return -1
				else:
					return -1
		else:
			return val
	else:
		return val

def remove_duplicates(los_df):

	pre_len = len(los_df)
	secure_log("::::: Dropping duplicates. PRE-length: {}".format(len(los_df)))

	if ('enc_velocify_lead_id' not in list(los_df.columns)):
		los_df['enc_velocify_lead_id'] = None

	#Set lead_id to -1 if null
	los_df['enc_velocify_lead_id'] = [-1 if pd.isnull(x) == True else re.sub("[a-zA-Z@]","",str(x).replace('.0','')) for x in los_df['enc_velocify_lead_id'] ] 
	los_df['enc_velocify_lead_id'] = [-1 if x == '' else -1 if '.' in str(x) else -1 if '+' in str(x) else int(x) for x in los_df['enc_velocify_lead_id'] ]

	### Drop dups where velocify lead id exists
	loan_filter = los_df.loc[los_df['enc_velocify_lead_id'] != -1].copy(deep=True)

	if (len(loan_filter) > 0):
		loan_filter['lock_flag'] = (pd.isnull(loan_filter['milestone_date_lock']) == False)

		loan_filter = loan_filter.sort_values(['lock_flag', 'milestone_date_lock'], ascending=False)\
			.drop_duplicates(['enc_velocify_lead_id'])\
			.drop('lock_flag', 1).sort_values(['account_loan_id', 'enc_velocify_lead_id']).reset_index(drop=True)

		loan_nodup_df = los_df[los_df['enc_velocify_lead_id'].isin(list(loan_filter['enc_velocify_lead_id'])) == False]
		los_df = loan_nodup_df.append(loan_filter,ignore_index=True, sort=True).reset_index(drop=True).sort_values('account_loan_id')

		secure_log("::::: Dropped {} lead_id duplicates. POST-length: {}".format(pre_len - len(los_df), len(los_df)))
		pre_len = len(los_df)

	### Drop dups where velocify lead id does not exist
	loan_filter = los_df.loc[los_df['enc_velocify_lead_id'] == -1].copy(deep=True)

	if (len(loan_filter) > 0):
		loan_filter['lock_flag'] = (pd.isnull(loan_filter['milestone_date_lock']) == False)

		loan_filter = loan_filter.sort_values(['lock_flag', 'milestone_date_lock'], ascending=False)\
			.drop_duplicates(['account_loan_id'])\
			.drop('lock_flag', 1).sort_values(['account_loan_id']).reset_index(drop=True)

		loan_nodup_df = los_df[los_df['account_loan_id'].isin(list(loan_filter['account_loan_id'])) == False]
		los_df = loan_nodup_df.append(loan_filter,ignore_index=True, sort=True).reset_index(drop=True)
		
		secure_log("::::: Dropped {} loan id duplicates. POST-length: {}".format(pre_len - len(los_df), len(los_df)))

	return los_df

def report_to_rollbar(err, stack, exc_info, account):
	secure_log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")

	secure_log(str(err))
	secure_log(stack)
	secure_log(exc_info)

	sns = boto3.client('sns')
	
	py_err = { 'name': str(err), 'message': "EncompassDataArchException", 'type': "error", 'trace': {
		'exc_info': "\"{}\"".format( str(exc_info.replace("\"", "'") ) ), 
		'stack': "\"{}\"".format(str(stack).replace("\"", "'"))
		} 
	}
	
	rollbar_error = { 'error': py_err, 'referer': 'Encompass', 'account': account }
	rollbar_error = json.dumps(rollbar_error)

	response = sns.publish(
		TopicArn=os.environ["TOPIC_ROLLBAR"], 
		Subject="Production Error in Encompass Lambda",   
		Message=rollbar_error
	)
	secure_log("Response: {}".format(response))
