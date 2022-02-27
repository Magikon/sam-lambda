import sys
import os
import traceback
import multiprocessing as mp
import threading
mp.set_start_method('spawn', True)
import urllib3 as urllib3
import urllib
from pprint import pprint
import psycopg2
import string
import datetime
import re
import boto3
import time
import csv

from faker import Faker
import random
from fake_event_types import event_types 
fake = Faker()

event_count = 0
lead_id = 204616
log_id = 1331934
file_count = 0
dirname = './fake_csv/'


def get_value(data_type):
	if (data_type == "ID"):
		return random.randint(0,20)
	elif (data_type == "BIGID"):
		return random.randint(100,100000)
	elif (data_type == "EMAIL"):
		return fake.email()
	elif (data_type == "NAME"):
		return fake.name()
	elif (data_type == "TEXT"):
		return fake.text()
	elif (data_type == "TIMESTAMP"):
		frmt = '%Y-%m-%d %H:%M:%S'
		stime = datetime.datetime.strptime('2019-01-01 00:00:00', frmt)
		etime = datetime.datetime.strptime('2019-11-26 00:00:00', frmt)
		td = etime - stime
		random_date = random.random() * td + stime
		return random_date.strftime(frmt)
	elif (data_type == "BOOL"):
		return random.choice(["Success", "Failure"])
	else:
		return None



def generate_event():
	global event_count
	global lead_id
	global log_id
	global output

	rnd_event_limit = random.randint(4,8)

	if (event_count > rnd_event_limit):
		event_count = 0
		lead_id += 1

	rnd_type = random.choice(list(event_types.keys()))
	date_now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
	event = {
		"account_id": 10,
		"account_lead_id": lead_id,
		"log_type": rnd_type.capitalize(),
		"log_id": None,
		"log_date": None,
		"log_subtype_id": None,
		"log_subtype_name": None,
		"milestone_id": None,
		"milestone_name": None,
		"log_note": None,
		"log_user_id": None,
		"log_user_email": None,
		"log_user_name": None,
		"group_id": None,
		"group_name": None,
		"imported": None,
		"created_at": date_now,
		"updated_at": date_now,
		"log_type_id": get_value("ID"),		
		"modified_at": get_value("TIMESTAMP")
	}
	milestones = [{'MilestoneId': '0', 'MilestoneTitle': 'None'}, {'MilestoneId': '1', 'MilestoneTitle': 'Contact'}, {'MilestoneId': '2', 'MilestoneTitle': 'Qualification'}, {'MilestoneId': '4', 'MilestoneTitle': 'Conversion'}]
	milestone = random.choice(milestones)
	for i in event_types[rnd_type].values():
		if (i['field_name'] == 'milestone_id'):
			event['milestone_id'] = milestone['MilestoneId']
			event['milestone_name'] = milestone['MilestoneTitle']
		elif (i['field_name'] == 'log_id'):
			event['log_id'] = log_id
		else:
			event[i['field_name']] = get_value(i['field_type'])

	event_count += 1
	log_id += 1
	return event
	

def generate_events():
	global lead_id
	global log_id
	global file_count
	global dirname

	file_count += 1
	print("::::: File Count: {}".format(file_count))
	print("::::: Last Lead ID: {}".format(lead_id))
	print("::::: Last Log ID: {}".format(log_id))
	num_events = random.randint(9000,15000)
	print("::::: GENERATING {} events".format(num_events))
	events = []

	for i in range(num_events):
		events.append(generate_event())
	
	csv = insert_to_csv(events, dirname)
	if (csv is not None):
		upload_to_s3(csv, dirname)
	

def update(event, context):
	global output
	def howmany_within_range_rowonly(row, minimum=4, maximum=8):
		count = 0
		for n in row:
			if minimum <= n <= maximum:
				count = count + 1
		return count

	try:
		num_files = 3000

		print("::::: GENERATING {} files".format(num_files))
		for i in range(num_files):
			generate_events()
		print("::::: ALL DONE")
			

	except Exception as err:
		stack = traceback.format_exc()
		print(err)

		

def upload_to_s3(csv_file, dirname):
	print("::::: Uploading {} to S3".format(csv_file))

	s3 = boto3.resource('s3')
	s3.meta.client.upload_file(dirname + csv_file, os.environ["BUCKET"], 'events-tmp/{}/{}'.format('propair-bank',csv_file))


def insert_to_csv(events, dirname):
	try:
		if len(events) > 0:
			csv_columns = list(events[0].keys())
			csv_date = datetime.datetime.strftime(datetime.datetime.now(), "%Y-%m-%d_%H:%M:%S")

			csv_file = "{}_{}.csv".format('propair-bank', csv_date)
			try:
				with open(dirname + csv_file, 'w') as csvfile:
					writer = csv.DictWriter(
					csvfile, 
					fieldnames=csv_columns,
					delimiter="|"
					)
					writer.writeheader()
					for data in events:
						writer.writerow(data)

				print("::::: SUCCESSFULLY WROTE EVENTS TO CSV LOCATION: {}".format(dirname + csv_file))
				return csv_file
			except IOError as err:
				print("I/O error: ", err)
		else:
			print("There isn't anything to insert")

	except Exception as err:
		print("----> DB error: " + str(err))
		stack = traceback.format_exc()
