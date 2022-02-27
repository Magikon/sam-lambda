
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
		select eld.account_id, eld.account_lead_id, el.first_name_stated + ' ' + el.last_name_stated as name,
		eld.lead_datetime, el.campaign_group,
		eld.profile_id_first_assignment_user, eld.profile_id_user, el.day_phone, el.evening_phone
		from external_lead_details eld
		left join external_leads el on eld.account_id = el.account_id and eld.account_lead_id=el.account_lead_id
		where eld.account_id={}
		and eld.account_lead_id < 0
		and eld.lead_datetime > '{}';
	""".format(account_id, date_dt)

	cur.execute(sql)
	data = meld_columns(cur.fetchall(), [x.name for x in cur.description])

	return group_by_column(data, 'account_lead_id')

def get_events(cur, account_id, date_dt):
	sql = """
		select account_id, account_lead_id, log_subtype_name, log_subtype_id, log_type, log_user_email, 
		log_user_id, log_user_name, milestone_id, milestone_name, log_date
		from external_events
		where account_id={}
		and account_lead_id < 0
		and log_date > '{}'
		and log_type = 'Action';
	""".format(account_id, date_dt)

	cur.execute(sql)
	data = meld_columns(cur.fetchall(), [x.name for x in cur.description])

	return data

def strTimeProp(start, end, format, prop):
	"""Get a time at a proportion of a range of two formatted times.

	start and end should be strings specifying times formated in the
	given format (strftime-style), giving an interval [start, end].
	prop specifies how a proportion of the interval to be taken after
	start.  The returned time will be in the specified format.
	"""

	stime = time.mktime(start.timetuple())
	etime = time.mktime(end.timetuple())

	ptime = stime + prop * (etime - stime)

	return time.strftime(format, time.localtime(ptime))


def randomDate(start, end):
	return strTimeProp(start, end, '%Y-%m-%d %H:%M:%S', random.random())

def rand_phone():
	range_start = 10**(7)
	range_end = (10**8)-1
	n = str(randint(range_start, range_end))
	return '555-' + n[1:4] + '-' + n[4:]

def generate_call(lead, event, call_config, system):
	current_date = datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')
	lead_id = lead['account_lead_id']
	event_dt = event['log_date']

	if (system=="velocify"):
		agent = random.choice(call_config['agents'])
		template = Call({
			'attrib': {
				'AgentId': event['log_user_id'], 
				'CallDuration': '{}:{}'.format(random.randrange(1,10), random.randrange(0,60)), 
				'CallOrigin': random.choice(call_config['call_origin']), 
				'CallSegment': random.choice(call_config['call_segment']), 
				'CallTime': event['log_date'], 
				'Campaign': lead['campaign_group'], 
				'Group': fake.company_suffix() + ' ' + fake.company(), 
				'LeadFullName': lead['name'], 
				'LeadId': '{}'.format(lead_id), 
				'ProspectPhone': rand_phone(), 
				'RecordingUrl': '', 
				'Result': event['log_subtype_name'],
				'InboundPhone': random.choice([lead['day_phone'], lead['evening_phone']]),
				'User': event['log_user_name'],
				'WaitTime': '{}'.format(random.randrange(0,2))
			},
			'tag': 'Call'
		})
	elif(system=="incontact"):
		total_duration = random.randint(120, 360)
		call_dt = event_dt - timedelta(minutes=total_duration)
		system = random.choice(call_config['systems'])

		skill = random.choice(call_config['skill_names'])

		skill_split = skill.split('_')
		state_called = skill_split[0]
		group_log = skill_split[1] if len(skill_split) > 1 else None
		call_series = skill_split[2] if len(skill_split) > 2 else None

		campaign_split = lead['campaign_group'].split('|') if lead['campaign_group'] != None else []
		source_detail = campaign_split[0] if len(campaign_split) > 0 else None
		call_origin = campaign_split[1] if len(campaign_split) > 1 else None


		template = {
			'account_lead_id': '{}'.format(lead_id),
			'call_datetime': '{}'.format(event['log_date']),
			'call_agent_id': '{}'.format(event['log_user_id']),
			'agent_time': '{}'.format(random.randint(2, total_duration)),
			'call_duration': '{}'.format(random.randint(2, total_duration)),
			'campaign_name': lead['campaign_group'],
			'inbound_phone': random.choice([lead['day_phone'], lead['evening_phone']]),
			'result': event['log_subtype_name'],
			'agent_profile_name': event['log_user_name'],
			'wait_time': '{}'.format(random.randint(0, int(total_duration / 3))),
			'in_queue': '{}'.format(random.randint(0, int(total_duration / 4))),
			'pre_queue': '{}'.format(random.randint(0, int(total_duration / 4))),
			'post_queue': '{}'.format(random.randint(0, int(total_duration / 5))),
			'routing_time': '{}'.format(random.randint(0, int(total_duration / 6))),
			'prospect_source_id': '{}'.format(random.randint(1, 100000000)),
			'disp_code': '{}'.format(event['log_subtype_id']),
			'agent_disposition': '{}'.format(random.randint(-100, 2000)),
			'sys_class': '{}'.format(system['code']),
			'sys_class_name': system['name'],
			'source_name': skill,
			'role_group': 'Team {}'.format(random.randint(104, 1000)),
			'call_disp_duration': '{}'.format(total_duration),
			'abandon': random.choice(['Y', 'N']),
			'contact_name': lead['name'],
			'call_series': call_series,
			'group_log': group_log,
			'state_called': state_called,
			'source_detail': source_detail,
			'call_origin': call_origin
		}

	return template

class Call(object):
	def __init__(self, *initial_data, **kwargs):
		for dictionary in initial_data:
			for key in dictionary:
				setattr(self, key, dictionary[key])
		for key in kwargs:
			setattr(self, key, kwargs[key])


def main():
	try: 

		account = os.environ['ACCOUNT']
		days_ago = os.environ['DAYS_AGO']

		print("::::: Starting Call generation")
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

			sandbox_config = get_account_config(1, cache=False)
			account_config = get_account_config(account_id, cache=False)
			system = account_config['calls_software'] if 'calls_software' in account_config else 'incontact'


			date_dt = datetime.now() - timedelta(days=int(days_ago))

			agents = get_agents(cur, account_id)
			leads = get_leads(cur, account_id, date_dt)
			events = get_events(cur, account_id, date_dt)

			calls = []
			call_counter = -1
			contact_counter = -1
			for event in events:
				lead = leads[event['account_lead_id']][0] if event['account_lead_id'] in leads else None
				if (lead):
					call = generate_call(lead, event, sandbox_config['fake_calls_config'][system], system)
					call['account_id'] = account_id
					call['call_id'] = call_counter
					call['contact_id'] = contact_counter

					calls.append(call)
					call_counter -= 1
					contact_counter -= 1

			print("::::: Generated {} calls".format(len(calls)))

			unique = ['account_id', 'call_id']
			insert_to_db(calls, 'call_logs', cur, conn, unique_columns=unique)


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

