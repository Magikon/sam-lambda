
from faker import Faker
from collections import defaultdict
from datetime import datetime, timedelta

import os
import math
import re
import random
import xmltodict
import psycopg2
import requests 
import boto3
import traceback

from helpers.data_generator import DataGenerator
from pplibs.logUtils import get_account_config
from utils import meld_columns
from utils import group_by_column
from db_utils import insert_to_db

from event_types import event_types 

fake = Faker()
generator = DataGenerator()


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



def get_agents(cur, account_id):
	sql = """
		SELECT id as propair_id, account_id, agent_id, name_velocify, email from agent_profiles
		WHERE account_id={}
	""".format(account_id)
	cur.execute(sql)
	data = meld_columns(cur.fetchall(), [x.name for x in cur.description])

	return group_by_column(data, 'propair_id')

def get_leads(cur, account_id, date_dt):
	sql = """
		select account_id, account_lead_id, lead_datetime, profile_id_first_assignment_user, profile_id_user
		from external_lead_details 
		where account_id={}
		and account_lead_id < 0
		and lead_datetime > '{}'
	""".format(account_id, date_dt)

	cur.execute(sql)
	data = meld_columns(cur.fetchall(), [x.name for x in cur.description])

	return data

def sanitize_and_cast(val, to_data_type):
	if(val == None):
		return None
	elif( "INT" in to_data_type):
		#TODO: Sanitize before casting
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
			return val
	else:
		return val


def generate_event(lead, agent, log_type, count, dt, subtype = None):
	
	if subtype == None:
		if (log_type == 'creationlog'):
			subtype = {"id": None, "name": None, "milestone": None, "milestone_id": None}
		else:
			subtype = random.choice(event_types[log_type]["subtypes"]) 

	event = {
		"account_id": lead['account_id'],
		"account_lead_id": lead['account_lead_id'],
		"log_subtype_name": subtype["name"],
		"log_subtype_id": subtype["id"],
		"log_type": log_type.capitalize(),
		"log_date": str(dt),
		"milestone_id": subtype["milestone_id"],
		"milestone_name": subtype["milestone"],
		"log_note": None,
		"log_user_id": agent['agent_id'] if agent else None,
		"log_user_email": agent['email'] if agent else None,
		"log_user_name": agent['name_velocify'] if agent else None,
		"modified_at": str(dt)
	}

	if (log_type in event_types and 'log_note' in event_types[log_type]):
		event['log_note'] = fake.text()
		if (random.random() < 0.001):
			event['log_note'] = generator.insert_unicode(event['log_note'])
		elif(random.random() < 0.001):
			event['log_note'] = generator.add_random_char(event['log_note'])


	if (log_type == 'distribution' and count > 1):
		event['log_subtype_name'] = "Redistribute Lead Distribution CA{}".format(count)
		event['log_subtype_id'] = 53 + count
	
	if (log_type == 'status'):
		event['log_subtype_name'] = event['log_subtype_name'] + " " + str(count)
		event['log_subtype_id'] = 1 + count

	return event
	
def generate_event_sequence(lead, agents):

	seq_events=[]
	num_of_events = random.randint(3,7)

	#Start with Creation Log
	#seq_events.append(generate_event('creationlog'))

	log_count = -1
	log_dt = datetime.now()
	counts = {}

	for key in event_types:
		counts[key] = 0

	#Find agent
	if(lead["profile_id_user"] in agents): agent = agents[lead["profile_id_user"]][0]
	elif(lead["profile_id_first_assignment_user"] in agents):  agent = agents[lead["profile_id_first_assignment_user"]][0]
	else: agent = None

	#Start with Creation Log
	seq_events.append(generate_event(lead, agent, 'creationlog', 1, log_dt))
	probs = {
		'distribution': 0.7,
		'action': 0.8,
		'export': 0.1,
		'status': 0.6,
		'email': 0.7
	}
	for i in range(0, num_of_events):
		rnd_type = []
		while len(rnd_type) == 0:
			rnd_type = [x for x in event_types if random.random() < probs[x]]
		rnd_type = random.choice(rnd_type)
		counts[rnd_type] += 1
		log_count -= 1

		rand_h = random.randint(0,2)
		rand_m = random.randint(1,60)
		log_dt = log_dt + timedelta(hours=rand_h) + timedelta(minutes=rand_m)

		if (i == num_of_events - 1):
			# If final event
			# Generate a qualification log

			rnd_type = 'action'
			subtype = random.choice([x for x in event_types[rnd_type]['subtypes'] if x['milestone_id'] == 2])
			event = generate_event(lead, agent, rnd_type, counts[rnd_type], log_dt, subtype)
		else:
			event = generate_event(lead, agent, rnd_type, counts[rnd_type], log_dt)

		event["log_id"] = log_count

		seq_events.append(event)

	return seq_events
	

def main():
	try: 

		account = os.environ['ACCOUNT']
		days_ago = os.environ['DAYS_AGO']

		print("::::: Starting Event generation")
		print("::::: Account: {}".format(account))
		print("::::: Days ago: {}".format(days_ago))

		print('::::: Connecting to database')
		conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()
		print('::::: Successfully connected to database')

		sql = """
			SELECT id, name from accounts where name='{}'
		""".format(account)
		cur.execute(sql)

		account_id = cur.fetchone()

		if(account_id):
			account_id = account_id[0]
			account_config = get_account_config(account_id, cache=False)

			date_dt = datetime.now() - timedelta(days=int(days_ago))

			leads = get_leads(cur, account_id, date_dt)
			agents = get_agents(cur, account_id)

			events = []

			for lead in leads:
				events = events + generate_event_sequence(lead, agents)

			print("::::: Generated {} events".format(len(events)))


			sq = "SELECT log_type_id, log_type FROM log_type_lookup"
			cur.execute(sq)
			data = cur.fetchall()

			log_type_mapping =  [{'from': 'log_type', 'to': 'log_type_id', 'map': data}]

			defaults = {'log_subtype_id': -50}
			unique_columns = ['account_id', 'account_lead_id', 'log_id', 'log_subtype_id']


			insert_to_db(events, 'external_events', cur, conn, unique_columns=unique_columns, defaults=defaults, mapping=log_type_mapping)


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

