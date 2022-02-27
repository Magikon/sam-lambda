import sys
import os

import requests
import xml.etree.ElementTree as ET
import psycopg2
import traceback
import string
import datetime
import time
import re
import boto3
import json
import random as rnd
from operator import itemgetter
import numpy as np

from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log

sns = boto3.client('sns')

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

velocify_get_reports_url		= "https://service.leads360.com/ClientService.asmx/GetReports"
velocify_get_report_url		 = "https://service.leads360.com/ClientService.asmx/GetReportResultsWithoutFilters"
velocify_modify_lead_field_url = "https://service.leads360.com/ClientService.asmx/ModifyLeadField"

def sanitize_rec(n):
	return re.sub("(nr)|(ns)|(ne)", "n", str(n))

def get_accounts():
	secure_log("::::: Fetching Accounts with Support for Expanded Recommendation Strings")
	conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
	cur = conn.cursor()
	accounts = []
	
	try:
		query = 'SELECT id, name, velocify_username, velocify_password, velocify_rec_field_1_id FROM accounts;'
		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			accounts.append( { 'id': line[0], 'name': line[1],'velocify_username': line[2], 'velocify_password': line[3], 'velocify_rec_field_id': line[4]} )
	except:
		secure_log("I am unable to connect to the database")

	cur.close()
	conn.close()

	return accounts

def get_leads(account_id, timespan):
	conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
	cur = conn.cursor()
	leads = []

	timestamp = datetime.datetime.now() - datetime.timedelta(seconds=timespan)
	try:
		query = """SELECT customer_id, customer_lead_id, payload, state FROM leads WHERE customer_id = {} 
						AND customer_lead_id is not null
						AND state = 'NEW' AND is_valid_json(payload) 
						AND json_extract_path_text(payload, 'recommendation') NOT LIKE '%000y%' 
						AND json_extract_path_text(payload, 'recommendation') NOT LIKE '%0002r%'
						AND json_extract_path_text(payload, 'recommendation', true) <> '' 
						AND created_at < '{}' and created_at > '{}' LIMIT 150""".format(account_id, timestamp, '2019-11-15 00:00:00')
		
		secure_log(query)
		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			original_lead = { 'customer_id': line[0], 'customer_lead_id': line[1], 'payload': line[2], 'state': line[3] }
			leads.append(original_lead)
	except Exception as inst:
		secure_log("I am unable to connect to the database")
		secure_log(inst)
	
	cur.close()
	conn.close()
	return leads


def recommend(lead, percentage):
	secure_log("::::: Generating New Recommendation", account_lead_id=lead['customer_lead_id'])
	#TODO: Implementation
	data = json.loads(lead['payload'])
	#data['recommendation'] = data['recommendation'].replace(':CRy','')

	recs  = data['recommendation'].split(":")
	algo_version = recs.pop() #Removes last element (e.g. model version)
	cr_value = ''
	if ('CR' in recs[len(recs)-1]):
		cr_value = recs.pop()
	probs = data['probabilities'].replace('nan', '0.00').split(":")

	rec_objects = []
	for idx, prob in enumerate(probs): 
		rec = { 'recommendation': recs[idx], 'probability': probs[idx] }
		rec_objects.append(rec)

	rec_objects.pop(0) #First object will always be empty due to string splitting by ':'
	#print rec_objects

	###############################
	# Sort all objects FULL/RANDOM
	###############################
	sorted_rec_objects = sorted(rec_objects, key=itemgetter('probability'), reverse=True)
	sorted_rec_objects_FULL = [x for x in sorted_rec_objects if 'r' not in x['recommendation']]
	sorted_rec_objects_RANDOM = [x for x in sorted_rec_objects if 'r' in x['recommendation']]
	sorted_rec_objects_RANDOM_yes = [x for x in sorted_rec_objects if 'yr' in x['recommendation']]
	sorted_rec_objects_RANDOM_no = [x for x in sorted_rec_objects if 'nr' in x['recommendation']]

	###############################
	# Find Counts for FULL/RANDOM 
	###############################
	Ntotal	= len(sorted_rec_objects)
	Nrec = len([x['recommendation'] for x in sorted_rec_objects if 'y' in x['recommendation']])
	Ntotal_FULL = len([x['recommendation'] for x in sorted_rec_objects if 'r' not in x['recommendation']])
	Nrec_FULL = len([x['recommendation'] for x in sorted_rec_objects if ('r' not in x['recommendation']) & ('y' in x['recommendation'])])
	Ntotal_RANDOM = len([x['recommendation'] for x in sorted_rec_objects if 'r' in x['recommendation']])
	Nrec_RANDOM = len([x['recommendation'] for x in sorted_rec_objects if ('r' in x['recommendation']) & ('y' in x['recommendation'])])

	Nrequired = int(float(Ntotal)/100*percentage)
	Nrequired_FULL = int(float(Ntotal_FULL)/100*percentage)
	Nrequired_RANDOM = int(float(Ntotal_RANDOM)/100*percentage)

	secure_log('ORIGINAL Agents (All)   : Total,Recommended,Percentage . {:3d}.{:3d},{:3d}%'.format(Ntotal,Nrec,int(Nrec/Ntotal*100)))
	secure_log('ORIGINAL Agents (Full)  : Total,Recommended,Percentage . {:3d},{:3d},{:3d}% . EXPANDED Agents (Full)  : Total,Recommended,Percentage . {:3d},{:3d},{:3d}%'\
					.format(Ntotal_FULL,Nrec_FULL,int(Nrec_FULL/Ntotal_FULL*100),Ntotal_FULL,Nrequired_FULL,int(percentage)))
	if(Ntotal_RANDOM > 0):
		secure_log('ORIGINAL Agents (Random): Total,Recommended,Percentage . {:3d},{:3d},{:3d}% . EXPANDED Agents (Random): Total,Recommended,Percentage . {:3d},{:3d},{:3d}%'\
						.format(Ntotal_RANDOM,Nrec_RANDOM,int(Nrec_RANDOM/Ntotal_RANDOM*100),Ntotal_RANDOM,Nrequired_RANDOM,int(percentage)))

	new_rec_string = ''

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
					rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'ye' )
				else:
					rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'y' )
				Nadded_FULL = Nadded_FULL + 1
				#print('FULL . YES . {} . {}'.format(rec['recommendation'],Nadded_FULL))
			else:
				if 'n' not in rec['recommendation']:
					rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'ne' )
				else:
					rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'n' )
		
				#print('FULL . NO . {} . {}'.format(rec['recommendation'],Nadded_FULL))
			prob_bits.append( rec['probability'].replace('0.00', 'nan') )
			rec_order_bits.append( int( re.sub("[^0-9]", "", rec['recommendation']) ) )

	secure_log('Full  : Added Agents: . {:3d}'.format(Nrequired_FULL-Nrec_FULL))	
	#print(rec_bits)
	#print(rec_order_bits)
	#print(prob_bits)

	###################################
	# 2) Update RANDOM recommendations
	###################################
	Nadded_RANDOM_yes = 0
	for idx, rec in enumerate(sorted_rec_objects_RANDOM_yes): 
		rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'yr' )
		prob_bits.append( rec['probability'].replace('0.00', 'nan') )
		rec_order_bits.append( int( re.sub("[^0-9]", "", rec['recommendation']) ) )
		Nadded_RANDOM_yes = Nadded_RANDOM_yes + 1

		#print('RANDOM . YES . {} . {}'.format(rec['recommendation'],Nadded_RANDOM_yes))

	##############################
	# Randomly shuffle 'n' agents
	##############################
	Nadded_RANDOM_no = 0
	Nextra_RANDOM = Nrequired_RANDOM-Nrec_RANDOM
	rnd_values = [rnd.random() for x in sorted_rec_objects_RANDOM_no]
	idx_rnd = list(np.arange(0,len(sorted_rec_objects_RANDOM_no)))
	idx_rnd = rnd.sample(idx_rnd, len(idx_rnd))
	idx_rnd = rnd.sample(idx_rnd, len(idx_rnd))
	sorted_rec_objects_RANDOM_no = [sorted_rec_objects_RANDOM_no[x] for x in idx_rnd]
	for idx, rec in enumerate(sorted_rec_objects_RANDOM_no): 
		if Nadded_RANDOM_no < Nextra_RANDOM:
			if 'y' not in rec['recommendation']:
				rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'yre' )
			else:
				rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'yr' )
			Nadded_RANDOM_no = Nadded_RANDOM_no + 1
			#print('RANDOM . YES . {} . {}'.format(rec['recommendation'],Nadded_RANDOM_no))
		else:
			if 'n' not in rec['recommendation']:
				rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'nre' )
			else:
				rec_bits.append( re.sub("[^0-9]", "", rec['recommendation']) + 'nr' )
	
			#print('RANDOM . NO . {} . {}'.format(rec['recommendation'],Nadded_RANDOM_no))
		prob_bits.append( rec['probability'].replace('0.00', 'nan') )
		rec_order_bits.append( int( re.sub("[^0-9]", "", rec['recommendation']) ) )

	secure_log('Random: Added Agents: . {:3d}'.format(Nextra_RANDOM))	
	secure_log('ORIGINAL Agents: Total,FullY,FullN,RandomY,RandomN . {:3d},{:3d},{:3d},{:3d},{:3d}'.format(len(rec_bits),Nrec_FULL,\
										Ntotal_FULL-Nrec_FULL,len(sorted_rec_objects_RANDOM_yes),len(sorted_rec_objects_RANDOM_no)))
	secure_log('EXPANDED Agents: Total,FullY,FullN,RandomY,RandomN . {:3d},{:3d},{:3d},{:3d},{:3d}'.format(len(rec_bits),Nrequired_FULL,\
										len(sorted_rec_objects_FULL)-Nrequired_FULL,Nadded_RANDOM_yes+Nadded_RANDOM_no,\
										len(sorted_rec_objects_RANDOM_no)-Nextra_RANDOM))

	sorted_rec_bits  = [x for _,x in sorted(zip(rec_order_bits, rec_bits))]
	sorted_rec_bits  = [x for _,x in sorted(zip(rec_order_bits, rec_bits))]
	sorted_prob_bits = [x for _,x in sorted(zip(rec_order_bits, prob_bits))]

	if (cr_value != ''):
		sorted_rec_bits.append( cr_value )

	sorted_rec_bits.append( algo_version )
	sorted_rec_bits.insert(0, '')
	sorted_prob_bits.insert(0, '')
	
	sorted_rec_string   = ':'.join( rec_bits )
	sorted_probs_string = ':'.join( prob_bits )
	
	
	new_rec_string =  ':'.join( sorted_rec_bits )
	new_probs_string = ':'.join( sorted_prob_bits )

	secure_log("ORIGINAL Rec String: {}".format(data['recommendation']))
	secure_log("EXPANDED Rec String: {}".format(new_rec_string))
	secure_log("Sorted Rec String: {}".format( sorted_rec_string))
	secure_log("Sorted Probs String: {}".format(sorted_probs_string))

	return({'recommendation': new_rec_string, 'probabilities': new_probs_string})

def write_new_recommendation_to_velocify(account, lead, recommendation):
	secure_log("::::: Writing New Recommendation to Velocify") 
	payload = {'username': account['velocify_username'], 'password': account['velocify_password'],
				'leadId': lead['customer_lead_id'], 'fieldId': account['velocify_rec_field_id'], 'newValue': recommendation}

	r = requests.post(velocify_modify_lead_field_url, data = payload)
	if (r.status_code == 200):
		secure_log("::::: Successfully modified lead {}".format(lead['customer_lead_id']))
	else:
		secure_log("::::: Failed to modify lead {}".format(lead['customer_lead_id']))
	

def write_new_recommendation_to_db(lead, recommendation):
	secure_log("::::: Writing New Recommendation to DB")
	lead['payload'] = json.loads(lead['payload'])
	lead['payload']['expandedRecommendation'] = recommendation['recommendation']
	lead['payload']['expandedProbabilities'] = recommendation['probabilities']
	lead['payload']['trimmedRecommendation'] = (":" + ':'.join(map(sanitize_rec, [x for x in recommendation['recommendation'].split(":") if "N" in x.upper() or "CR" in x.upper()])) \
					+ ":" + recommendation['recommendation'].split(":")[len(recommendation['recommendation'].split(":")) - 1]).replace('::',':')
	
	conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
	cur = conn.cursor()

	try:
		query = "UPDATE leads SET payload = $${}$$, state=$$EXPANDED$$, updated_at=$${}$$ WHERE customer_lead_id={} AND customer_id={};".format(json.dumps(lead['payload']), datetime.datetime.now(), lead['customer_lead_id'], lead['customer_id'])
		cur.execute(query)
		conn.commit()
	except Exception as inst:
		secure_log("I am unable to connect to the database")
		secure_log(inst)

	cur.close()
	conn.close()

	return lead

def log(lead, account):
	secure_log("::::: Logging Lead")
	response = sns.publish(
		Message  = json.dumps( { 'lead': lead, 'account': account, 'reassignment': False, 'expansion': True} ),
		Subject  = "Log Lead",
		TopicArn = os.environ["TOPIC_LOG_LEAD"]
	)

	secure_log(response)


def update(event, context):
	current_account = ""
	try:
		accounts = get_accounts()
		
		for account in accounts:
			current_account = account['name']
			account_config = get_account_config(account['id'], cache=False)

			run_expansion = account_config['rec_expansion'][0]['run_rec_expansion'] if 'rec_expansion' in account_config else False
			
			if (run_expansion):
				secure_log("::::: Running rec expansion for {}.".format(account['name']))
				expand_rec_after_seconds = int(account_config['rec_expansion'][0]['expand_rec_after_seconds']) if 'expand_rec_after_seconds' in account_config['rec_expansion'][0] else False
				expand_rec_by_percentage = int(account_config['rec_expansion'][0]['expand_rec_by_percentage']) if 'expand_rec_by_percentage' in account_config['rec_expansion'][0] else False
			
				if (not expand_rec_after_seconds):
					secure_log("::::: ERROR! No expand_rec_after_seconds configuration set for {}, please set value in DynamoDB. Skipping..".format(account['name']))
					continue
				if (not expand_rec_by_percentage):
					secure_log("::::: ERROR! No expand_rec_by_percentage configuration set for {}, please set value in DynamoDB. Skipping..".format(account['name']))
					continue

				leads = get_leads(account['id'], expand_rec_after_seconds)
				if (len(leads) > 0):
					secure_log("::::: Retrieved {} leads".format(len(leads)))
					err_count = 0
					for lead in leads:
						try:
							recommendation  = recommend(lead, expand_rec_by_percentage)
							modified_lead = write_new_recommendation_to_db(lead, recommendation)

							secure_log(modified_lead['payload']['trimmedRecommendation'])
							log( { 'lead': modified_lead['payload'], 'customer': account['name'] }, account )
							write_new_recommendation_to_velocify(account, lead, modified_lead['payload']['trimmedRecommendation'])
						except Exception as e:
							err_count += 1
							secure_log("::::: Expansion Error: " + str(e), None, lead['customer_lead_id'])

					secure_log("::::: All Done")
					if (err_count > (int(len(leads)) * 0.5)):
						raise Exception("Expansion Errors surpassed 50% threshold!")
				else: 
					secure_log("::::: No new leads found for {}".format(account['name']))
			else:
				secure_log("::::: run_rec_expansion off for {}.. Skipping.".format(account['name']))
				
	except Exception as e:
		stack = traceback.format_exc()
		secure_log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")
		secure_log(e)
		secure_log(stack)
		secure_log(sys.exc_info())

		sns = boto3.client('sns')
		py_err = { 'name': "PythonException", 'message': str(e), 'type': "error", 'trace': {'event': event, 'exc_info': "\"{}\"".format( str(sys.exc_info()).replace("\"", "'")), 'stack': "\"{}\"".format(str(stack).replace("\"", "'"))}}
		current_account = "NO_ACCOUNT" if current_account == "" else current_account
		rollbar_error = { 'error': py_err, 'referer': 'ExpandRecommendation', 'account': current_account }

		rollbar_error = json.dumps(rollbar_error)

		secure_log(sns.publish(
			TopicArn=os.environ["TOPIC_ROLLBAR"], 
			Subject="Production Error in Recommend Lambda",   
			Message=rollbar_error
		))
