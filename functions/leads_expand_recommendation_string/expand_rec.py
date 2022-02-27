import datetime
import json
import os
import random as rnd
import re
import sys
import traceback
from operator import itemgetter

import boto3
import numpy as np
import pandas as pd
import psycopg2
from kafka import KafkaProducer

from pplibs.kafka_utils import get_kafka_config
from pplibs.logUtils import secure_log, get_account_config
from pplibs.customErrors import ProPairError, report_to_rollbar

pd.set_option('display.width', 1000)
pd.set_option('display.max_colwidth', 200)
pd.set_option('display.max_rows', 400)

sns = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

db_host = os.environ["REDSHIFT_ENDPOINT"]
db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = os.environ["REDSHIFT_DB_PORT"]

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

config = get_kafka_config()
host = config["host"]
env = os.environ["ENV"]


def on_send_success(record_metadata):
	pass


def on_send_error(excp):
	print('Something went wrong', exc_info=excp)


def fetch_expansion_leads(account_id, cur, after_seconds):
	leads = []
	secure_log(":::: Retrieving leads for expansion")
	try:
		dt_start = '2019-11-15 00:00:00'
		dt_end = datetime.datetime.now() - datetime.timedelta(seconds=after_seconds)

		query = f"""
			SELECT customer_id, customer_lead_id, payload, state, created_at FROM leads WHERE customer_id = {account_id}
				AND state = 'NEW'
				AND is_valid_json(payload)
				AND json_extract_path_text(payload, 'recommendation') NOT LIKE '%000y%' 
				AND json_extract_path_text(payload, 'recommendation') NOT LIKE '%0002r%'
				AND json_extract_path_text(payload, 'recommendation', true) <> '' 
				AND created_at between '{dt_start}' and '{dt_end}' LIMIT 20000;
		"""

		secure_log(query)
		cur.execute(query)
		data = cur.fetchall()
		lead_df = pd.DataFrame(data, columns=[desc[0] for desc in cur.description])

		secure_log("::::: Retrieved {} leads".format(len(lead_df)))
	except Exception as err:
		msg = "Unable to retrieve leads for expansion {}".format(err)
		secure_log(msg)
		raise Exception(msg)

	return lead_df


def sanitize_rec(n):
	return re.sub("(nr)|(ns)|(ne)", "n", str(n))


def trim_rec(rec, params):
	filter_string = "N" if params['exclude_y'] else "Y" if params['exclude_n'] else None

	if filter_string:
		return (":" + ':'.join(
			map(sanitize_rec, [x for x in rec.split(":") if filter_string in x.upper() or "CR" in x.upper()])) \
				+ ":" + rec.split(":")[-1]).replace('::', ':')
	else:
		return rec


def recommend(recommend, probability, payload, percentage, exclusion_params):
	recs = recommend.split(":")
	algo_version = recs.pop()  # Removes last element (e.g. model version)
	cr_value = ''
	if ('CR' in recs[len(recs) - 1]):
		cr_value = recs.pop()
	probs = probability.replace('nan', '0.00').split(":")

	rec_objects = []
	for idx, prob in enumerate(probs):
		rec = {'recommendation': recs[idx], 'probability': probs[idx]}
		rec_objects.append(rec)

	rec_objects.pop(0)  # First object will always be empty due to string splitting by ':'

	###############################
	# Sort all objects FULL/RANDOM
	###############################
	sorted_rec_objects = sorted(rec_objects, key=itemgetter('probability'), reverse=True)
	sorted_rec_objects_FULL = [x for x in sorted_rec_objects if 'r' not in x['recommendation']]
	sorted_rec_objects_RANDOM_yes = [x for x in sorted_rec_objects if 'yr' in x['recommendation']]
	sorted_rec_objects_RANDOM_no = [x for x in sorted_rec_objects if 'nr' in x['recommendation']]

	###############################
	# Find Counts for FULL/RANDOM
	###############################
	Ntotal_FULL = len([x['recommendation'] for x in sorted_rec_objects if 'r' not in x['recommendation']])
	Ntotal_RANDOM = len([x['recommendation'] for x in sorted_rec_objects if 'r' in x['recommendation']])
	Nrec_RANDOM = len([x['recommendation'] for x in sorted_rec_objects if
					   ('r' in x['recommendation']) & ('y' in x['recommendation'])])

	Nrequired_FULL = int(float(Ntotal_FULL) / 100 * percentage)
	Nrequired_RANDOM = int(float(Ntotal_RANDOM) / 100 * percentage)

	rec_order_bits = []
	rec_bits = []
	prob_bits = []

	#################################
	# 1) Update FULL recommendations
	#################################
	Nadded_FULL = 0
	for idx, rec in enumerate(sorted_rec_objects_FULL):
		if 'r' not in rec['recommendation']:
			if Nadded_FULL < Nrequired_FULL:
				if 'y' not in rec['recommendation']:
					rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'ye')
				else:
					rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'y')
				Nadded_FULL = Nadded_FULL + 1
			else:
				if 'n' not in rec['recommendation']:
					rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'ne')
				else:
					rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'n')

			prob_bits.append(rec['probability'].replace('0.00', 'nan'))
			rec_order_bits.append(int(re.sub("[^0-9]", "", rec['recommendation'])))

	###################################
	# 2) Update RANDOM recommendations
	###################################
	Nadded_RANDOM_yes = 0
	for idx, rec in enumerate(sorted_rec_objects_RANDOM_yes):
		rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'yr')
		prob_bits.append(rec['probability'].replace('0.00', 'nan'))
		rec_order_bits.append(int(re.sub("[^0-9]", "", rec['recommendation'])))
		Nadded_RANDOM_yes = Nadded_RANDOM_yes + 1

	##############################
	# Randomly shuffle 'n' agents
	##############################
	Nadded_RANDOM_no = 0
	Nextra_RANDOM = Nrequired_RANDOM - Nrec_RANDOM
	idx_rnd = list(np.arange(0, len(sorted_rec_objects_RANDOM_no)))
	idx_rnd = rnd.sample(idx_rnd, len(idx_rnd))

	sorted_rec_objects_RANDOM_no = [sorted_rec_objects_RANDOM_no[x] for x in idx_rnd]
	for idx, rec in enumerate(sorted_rec_objects_RANDOM_no):
		if Nadded_RANDOM_no < Nextra_RANDOM:
			if 'y' not in rec['recommendation']:
				rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'yre')
			else:
				rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'yr')
			Nadded_RANDOM_no = Nadded_RANDOM_no + 1
		else:
			if 'n' not in rec['recommendation']:
				rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'nre')
			else:
				rec_bits.append(re.sub("[^0-9]", "", rec['recommendation']) + 'nr')

		prob_bits.append(rec['probability'].replace('0.00', 'nan'))
		rec_order_bits.append(int(re.sub("[^0-9]", "", rec['recommendation'])))

	sorted_rec_bits = [x for _, x in sorted(zip(rec_order_bits, rec_bits))]
	sorted_prob_bits = [x for _, x in sorted(zip(rec_order_bits, prob_bits))]

	if (cr_value != ''):
		sorted_rec_bits.append(cr_value)

	sorted_rec_bits.append(algo_version)
	sorted_rec_bits.insert(0, '')
	sorted_prob_bits.insert(0, '')

	new_rec_string = ':'.join(sorted_rec_bits)
	new_probs_string = ':'.join(sorted_prob_bits)
	trimmed_rec = trim_rec(new_rec_string, exclusion_params)

	p = json.loads(payload).copy()
	p['expandedRecommendation'] = new_rec_string
	p['expandedProbabilities'] = new_probs_string
	p['trimmedRecommendation'] = trimmed_rec

	return [new_rec_string, new_probs_string, trimmed_rec, json.dumps(p)]


def expand(event, context):
	try:
		if ('Records' in event):
			account_meta = json.loads(event['Records'][0]['Sns']['Message'])
		else:
			account_meta = event

		account_id = account_meta['account_id']
		account_config = get_account_config(account_id)

		secure_log("::::: Initiating Expand Rec Function", configuration=account_config)

		secure_log("::::: Connecting to database..")
		conn = psycopg2.connect(
			"dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()

		# Get account info if missing
		if any(x not in account_meta for x in ['velocify_username', 'velocify_password', 'account']):
			secure_log("::::: Account data missing! Fetching from DB")
			sq = 'SELECT name,velocify_username, velocify_password from accounts where id = {}'.format(account_id)
			cur.execute(sq)
			account_data = cur.fetchone()
			account_meta['account'] = account_data[0]
			account_meta['velocify_username'] = account_data[1]
			account_meta['velocify_password'] = account_data[2]

		# Extract expansion parameters

		try:
			config_variables = account_config['rec_expansion']['variables']
		except Exception:
			raise Exception(
				"Unable to find configuration in DynamoDB: <rec_expansion.variables>")

		try:
			expand_rec_after_seconds = int(config_variables['expand_rec_after_seconds'])
		except Exception:
			raise Exception(
				"Unable to find configuration in DynamoDB: <rec_expansion.variables.expand_rec_after_seconds>")

		try:
			expand_rec_by_percentage = int(config_variables['expand_rec_by_percentage'])
		except Exception:
			raise Exception(
				"Unable to find configuration in DynamoDB: <rec_expansion.variables.expand_rec_by_percentage>")

		# Fetch non-expanded leads
		leads_df = fetch_expansion_leads(account_meta['account_id'], cur, expand_rec_after_seconds)

		
		def extract_json(s):
			item = {
				'id': None,
				'recommendation':'',
				'probabilities': '',
				'custom_rank_bin': 0,
				'custom_rank': None
			}
			try: 
				data = json.loads(s)
				item['id'] = int(data['id'])
				item['recommendation'] = data['recommendation'] if 'recommendation' in data else ''
				item['probabilities'] = data['probabilities'] if 'probabilities' in data else ''
				item['custom_rank_bin'] = data['custom_rank_bin'] if 'custom_rank_bin' in data else 0
				item['custom_rank'] = data['custom_rank'] if 'custom_rank' in data else None
			except:
				pass

			return item

		leads_df = leads_df[(pd.isnull(leads_df['customer_lead_id']) == False) & (leads_df['customer_lead_id'] != 0)]
		leads_df = leads_df.merge(pd.DataFrame(extract_json(d) for d in leads_df['payload']), how='left',
								  left_on='customer_lead_id', right_on='id')

		secure_log("::::: Filtering out randoms and errors")
		leads_df = leads_df.loc[(leads_df['recommendation'].str.contains('000y') == False) & (
				leads_df['recommendation'].str.contains('0002r') == False) \
								& (leads_df['recommendation'] != '') & (pd.isnull(leads_df['recommendation']) == False)]
		secure_log(f"::::: Post filter: {len(leads_df)}")

		leads_df = leads_df.sort_values(by=['created_at']).drop_duplicates(subset=['customer_id', 'customer_lead_id'])
		secure_log(f"::::: Post drop duplicates length: {len(leads_df)}")

		if len(leads_df) > 0:
			secure_log("::::: Expanding Recommendations")
			try:
				exclusion_params = {
					'exclude_n': account_config['prospect_matching']['variables']['exclude_rec_n'],
					'exclude_y': account_config['prospect_matching']['variables']['exclude_y']
				}
			except KeyError as e:
				secure_log("::::: Exclusion params not set. Defaulting to Exclude Y's")
				exclusion_params = {'exclude_y': True, 'exclude_n': False}

			leads_df['expandedRecommendation'] = None
			leads_df['expandedProbabilities'] = None
			leads_df['trimmedRecommendation'] = None
			leads_df[['expandedRecommendation', 'expandedProbabilities', 'trimmedRecommendation', 'payload']] = [
				recommend(x, y, z, expand_rec_by_percentage, exclusion_params) for x, y, z in
				zip(leads_df['recommendation'], leads_df['probabilities'], leads_df['payload'])]

			# Standardizing Fields
			leads_df['account'] = account_meta['account']
			leads_df['account_id'] = account_meta['account_id']
			leads_df['account_lead_id'] = leads_df['customer_lead_id']
			leads_df['state'] = "EXPANDED"
			leads_df['propair_id'] = '0'
			leads_df['ref_id'] = 0
			leads_df['error'] = 0
			leads_df['rec_code'] = [x.split(':')[-1] for x in leads_df['recommendation']]
			leads_df['custom_rank_value'] = leads_df['custom_rank']
			leads_df['custom_rank_timeframe'] = 1
			leads_df['reassignment'] = 0
			leads_df['expansion'] = 1
			leads_df['updated_at'] = datetime.datetime.now()

			producer = KafkaProducer(bootstrap_servers=[host],
									 acks=config["acks"],
									 retries=config["retries"],
									 security_protocol='SASL_PLAINTEXT',
									 sasl_mechanism='PLAIN',
									 sasl_plain_username=config["user"],
									 sasl_plain_password=k_pass,
									 linger_ms=1000,
									 batch_size=32 * 1024,
									 value_serializer=lambda x: json.dumps(x).encode('utf-8')
									 )

			print("::: Producer created :::")

			n = 1000 #chunk row size
			secure_log(f"::::: Chunk row size: {n}")

			list_df = [leads_df[i:i+n] for i in range(0,leads_df.shape[0],n)]
			dt_n = datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")

			for i, df in enumerate(list_df):

				file_name = f'expansions_{dt_n}_part_{i}.json'
				file_path = f'expansions/{account_meta["account"]}/{file_name}'
				s3 = boto3.resource('s3')

				s3.Bucket(os.environ["BUCKET"]).put_object(Key=f'{file_path}', Body=df.to_json(orient='records', date_format='iso'))
				secure_log(f"::::: [{i} of {len(list_df)}] Uploaded file to s3: {file_path}")

				# send messages
				message_meta = {
					"account_id": account_meta['account_id'],
					"account_name": account_meta['account'],
					"bucket": os.environ["BUCKET"],
					"file_path": file_path
				}

				k = bytes(int(account_meta['account_id']))
				producer.send('expansions', key=k, value=message_meta). \
					add_callback(on_send_success). \
					add_errback(on_send_error)

				secure_log(f":::: [{i} of {len(list_df)}] File [{file_name}] Sent to Stream")

			secure_log(f"::::: {len(list_df)} Expansion files sent to stream")

			# block until all sync messages are sent
			producer.flush()
			producer.close()
		else:
			secure_log("::::: No leads available to expand!")

	except Exception as e:
		secure_log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")

		secure_log(str(e))
		secure_log(traceback.format_exc())
		secure_log(sys.exc_info())

		py_err = ProPairError(e, "ExpandRecommendation", exec_info=sys.exc_info(), stack=traceback.format_exc())
		try:
			account_meta = json.loads(event['Records'][0]['Sns']['Message'])
			account = account_meta['account']
			py_err.account = account
			report_to_rollbar(rollbar_topic, py_err)
		except Exception as err:
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)
