from faker import Faker
fake = Faker()
import re
import random
from random import randint
import json
import boto3

from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log

import time
from datetime import datetime, timedelta

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

def generate_call(lead, call_config, system):
	current_date = datetime.now().strftime('%m/%d/%Y %I:%M:%S %p')
	lead_id = lead['account_lead_id']
	lead_dt = lead['lead_datetime']
	max_dt = lead_dt + timedelta(hours=random.randint(1,72))

	if (system=="velocify"):
		agent = random.choice(call_config['agents'])
		template = Call({
			'attrib': {
				'Id': '{}'.format(random.randint(1, 100000000)),
				'AgentId': agent['agent_id'], 
				'CallDuration': '{}:{}'.format(random.randrange(1,10), random.randrange(0,60)), 
				'CallOrigin': random.choice(call_config['call_origin']), 
				'CallSegment': random.choice(call_config['call_segment']), 
				'CallTime': randomDate(lead_dt, max_dt), 
				'Campaign': fake.company_suffix() + ' ' + fake.company(), 
				'ContactId': '{}'.format(random.randrange(600000,700000)), 
				'Group': fake.company_suffix() + ' ' + fake.company(), 
				'InboundPhone': lead['phone'], 
				'LeadFullName': lead['name'], 
				'LeadId': '{}'.format(lead_id), 
				'ProspectPhone': rand_phone(), 
				'RecordingUrl': '', 
				'Result': random.choice(call_config['call_segment']),
				'TargetNumber': rand_phone(),
				'User': agent['name'],
				'WaitTime': '{}'.format(random.randrange(0,2))
			},
			'tag': 'Call'
		})
	elif(system=="incontact"):
		random_call_dt = datetime.strptime(randomDate(lead_dt, max_dt), '%Y-%m-%d %H:%M:%S')
		total_duration = random.randint(120, 360)
		agent = random.choice(call_config['agents'])
		system = random.choice(call_config['systems'])
		skill = random.choice(call_config['skill_names'])

		template = {
			'ExternalID': '{}'.format(lead_id),
			'Contact_ID': '{}'.format(random.randint(1, 100000000)),
			'Master_Contact_ID': '{}'.format(random.randint(1, 100000000)),
			'Start_Date': random_call_dt.strftime('%m/%d/%Y'),
			'start_time': random_call_dt.strftime('%H:%M:%S'),
			'Agent_No': '{}'.format(agent['agent_id']),
			'Agent_Time': '{}'.format(random.randint(2, total_duration)),
			'CallDuration': '{}'.format(random.randint(2, total_duration)),
			'Campaign_Name': random.choice(call_config['campaign_names']),
			'Skill_Name': skill,
			'Contact_Name': lead['name'],
			'PhoneNumber': lead['phone'],
			'Disp_Name': random.choice(call_config['disp_names']),
			'Agent_Name': agent['name'],
			'Hold_Time': '{}'.format(random.randint(0, int(total_duration / 3))),
			'InQueue': '{}'.format(random.randint(0, int(total_duration / 4))),
			'PreQueue': '{}'.format(random.randint(0, int(total_duration / 4))),
			'PostQueue': '{}'.format(random.randint(0, int(total_duration / 5))),
			'RoutingTime': '{}'.format(random.randint(0, int(total_duration / 6))),
			'ProspectiveContactSourceID': '{}'.format(random.randint(1, 100000000)),
			'Disp_Code': '{}'.format(random.randint(-100, 2000)),
			'AgentDisposition': '{}'.format(random.randint(-100, 2000)),
			'SystemClassification': '{}'.format(system['code']),
			'SystemClassificationName': system['name'],
			'SourceName': skill,
			'Team_Name': 'Team {}'.format(random.randint(104, 1000)),
			'Total_Time_Plus_Disposition': '{}'.format(total_duration),
			'Abandon': random.choice(['Y', 'N'])
		}

	return template

class Call(object):
	def __init__(self, *initial_data, **kwargs):
		for dictionary in initial_data:
			for key in dictionary:
				setattr(self, key, dictionary[key])
		for key in kwargs:
			setattr(self, key, kwargs[key])

def generate(system, cur):
	secure_log("::::: Generating fake calls")
	call_config = get_account_config(10)['fake_calls_config'][system]

	days_ago = call_config['days_ago'] if 'days_ago' in call_config else None
	min_ago = call_config['min_ago'] if 'min_ago' in call_config else None

	lead_groups = []
	if (days_ago):
		for day in days_ago:
			sql = """
				SELECT (el.first_name_stated + ' ' + el.last_name_stated) as name, el.day_phone, eld.account_lead_id, eld.lead_datetime FROM external_lead_details eld
				LEFT OUTER JOIN external_leads el
				ON eld.account_lead_id = el.account_lead_id
				AND eld.account_id = el.account_id
				WHERE eld.account_id=10
				AND (eld.lead_datetime::timestamp > convert_timezone('UTC', 'PST', getdate() - interval '{} day'))
				AND (eld.lead_datetime::timestamp < convert_timezone('UTC', 'PST', getdate() - interval '{} day'))
				ORDER BY lead_datetime;
			""".format(day, int(day) -1)
			cur.execute(sql)
			data = cur.fetchall()
			leads_arr = []
			for d in data:
				leads_arr.append({"name": d[0], "phone": d[1], "account_lead_id": d[2], "lead_datetime": d[3]})
			lead_groups.append(leads_arr)

	elif(min_ago):
		sql = """
			SELECT (el.first_name_stated + ' ' + el.last_name_stated) as name, el.day_phone, eld.account_lead_id, eld.lead_datetime FROM external_lead_details eld
			LEFT OUTER JOIN external_leads el 
			ON eld.account_lead_id = el.account_lead_id
			AND eld.account_id = el.account_id
			WHERE eld.account_id=10
			AND (eld.lead_datetime::timestamp > convert_timezone('UTC', 'PST', (getdate() - interval '{} min')))
			ORDER BY lead_datetime;
		""".format(min_ago)
		cur.execute(sql)
		data = cur.fetchall()
		leads_arr = []
		for d in data:
			leads_arr.append({"name": d[0], "phone": d[1], "account_lead_id": d[2], "lead_datetime": d[3]})
		lead_groups.append(leads_arr)
	else:
		raise Exception("No days_ago or min_ago specified in account_config")
	
	if (days_ago):
		for i, d in enumerate(days_ago):
			secure_log("::::: Retrieved {} leads from {} days ago".format(len(lead_groups[i]), d))
	elif(min_ago):
		secure_log("::::: Retrieved leads from {} min ago".format(min_ago))

	calls = []

	for lead in [item for sublist in lead_groups for item in sublist]:
		num_of_calls = random.randint(1, 3) if days_ago else 1
		for x in range(0, num_of_calls):
			calls.append(generate_call(lead, call_config, system))

	secure_log("::::: Generated {} calls".format(len(calls)))
	return calls
