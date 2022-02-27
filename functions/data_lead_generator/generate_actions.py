from faker import Faker
from lxml import etree
import os
import math
import re
import random
import xmltodict
import psycopg2
import requests 
import boto3
import json

from helpers.velocify import Velocify
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from utils import meld_columns

from datetime import datetime, timedelta
from pytz import timezone
import pytz

fake = Faker()

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]),
	WithDecryption=True
) 

db_pass = response['Parameter']['Value']
db_host = os.environ["REDSHIFT_ENDPOINT"]
db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = os.environ["REDSHIFT_DB_PORT"]

velocify_add_action_url = "https://service.leads360.com/ClientService.asmx/AddLeadAction"
velocify_get_leads_url = "https://service.leads360.com/ClientService.asmx/GetLeads"

def get_credentials(cur):
    print("::::: Fetching Account credentials")
    sql = """
        SELECT velocify_username, velocify_password from accounts where id = 8;
    """
    cur.execute(sql)

    return meld_columns(cur.fetchall(), [x.name for x in cur.description])[0]


def main(event, _context):
    secure_log(":::: Invoking Generate Actions Function")

    secure_log('::::: Connecting to database')
    conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
    secure_log('::::: Successfully connected to database')
    cur = conn.cursor()

    credentials = get_credentials(cur)
    sandbox_config = get_account_config(1, cache=False)

    velocify = Velocify(credentials['velocify_username'], credentials['velocify_password'])
    action_types = velocify.get_action_types()

    milestones = velocify.get_milestones()
    #append milestone config
    milestones = {key: {**value, **sandbox_config['milestone_config'][key]} for key, value in milestones.items()}

    #pick milestone based on probabilities
    milestone = random.choices(population=list(milestones.values()), weights=[float(x['prob']) for x in milestones.values()])[0]


    secure_log(":::: Generating {} actions".format(milestone['MilestoneTitle']))

    end_dt =  datetime.now(tz=pytz.utc).astimezone(timezone('US/Pacific')) - timedelta(hours=int(milestone['offset']))
    start_dt = end_dt - timedelta(hours=int(milestone['length']))
    leads = velocify.get_leads(start_dt.strftime('%m/%d/%Y %H:%M:%S'), end_dt.strftime('%m/%d/%Y %H:%M:%S'))

    for lead in leads:
        rand_action = random.choice(action_types[milestone['MilestoneId']])

        payload = {
            "username":  velocify.username,
            "password":  velocify.password,
            "leadId": lead['Id'],
            "actionTypeId": rand_action['ActionTypeId'],
            "actionNote": ''
        }
        secure_log("::::: [{}] {}".format(lead['Id'], rand_action['ActionTypeTitle']))
        re = requests.post(velocify_add_action_url, data=payload)

if __name__ == "__main__":
    main({}, {})
