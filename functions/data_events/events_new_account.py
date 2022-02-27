import sys
import os
import traceback

sys.path.insert(0, './lib/idna')
sys.path.insert(0, './lib/python-certifi')
sys.path.insert(0, './lib/chardet')
sys.path.insert(0, './lib/urllib3')
sys.path.insert(0, './lib/requests')
sys.path.insert(0, './lib')

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
import time

from event_types import event_types 


#### Temporarily hardcoded account metadata:
velocify_get_milestones_url     = "https://service.leads360.com/ClientService.asmx/GetMilestones"
velocify_get_leads_url  = "https://service.leads360.com/ClientService.asmx/GetLeads"
velocify_get_lead_ids_span_url  = "https://service.leads360.com/ClientService.asmx/GetLeadIdsSpan"

db_name = os.environ["DB_NAME"]
db_user = os.environ["DB_USER"]

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
    Name="{}-database-password".format(os.environ["ENV"]), 
    WithDecryption=True
) 

db_pass = response['Parameter']['Value']
db_host = os.environ["DB_ENDPOINT"]

def update(event, context):

    try:
        account_id = event['account_id']
        start_date   = datetime.datetime.strptime(event['start_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M:%S %p") if 'start_date' in event else None
        end_date   = datetime.datetime.strptime(event['end_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M:%S %p") if 'end_date' in event else None
        minutes_from = event['fromNowMinutes'] if 'fromNowMinutes' in event else None

        print("::::: Fetching account")

        conn = psycopg2.connect('host={} dbname={} user={} password={}'.format(db_host, db_name, db_user, db_pass))
        cur = conn.cursor()
        sq = "SELECT * FROM accounts WHERE id={} AND velocify_username IS NOT NULL AND velocify_password IS NOT NULL".format(account_id)
        cur.execute(sq)
        response = cur.fetchone()
        
        if response != None:
            account = { 'id': response[0], 'name': response[1].upper(), 'velocify_username': response[3], 'velocify_password': response[4] }

            print("::::: Fetching Customer Milestones for {}".format(account['name']))
            
            payload = {'username':account['velocify_username'], 'password': account['velocify_password']}
            r = requests.post(velocify_get_milestones_url, data = payload)
            
            print("------------>")
            print(r.text)
            data = ET.fromstring(r.text)
            milestones = []
            for m in data:
                milestones.append(m.attrib)
            
            print("::::: Fetching {} Leads from {} to {}".format(account['name'], start_date, end_date))
            
            if (minutes_from):
                payload = {'username':account['velocify_username'], 'password': account['velocify_password'], 'fromNowMinutes': minutes_from}
                r = requests.post(velocify_get_lead_ids_span_url, data = payload)
            else:
                payload = {'username':account['velocify_username'], 'password': account['velocify_password'], 'from': start_date, 'to': end_date}
                r = requests.post(velocify_get_leads_url, data = payload)

            # defining a params dict for the parameters to be sent to the API
            
            # sending get request and saving the response as response object
            
            # extracting data in XML format
            events = []
            lead_id = None
            lead_modified_at = None
            leads = ET.fromstring(r.text)
            for lead in leads:
                lead_id = lead.attrib['Id']
                lead_modified_at = lead.attrib['ModifyDate']
                #TODO:
                #CREATE_DATE
                #MODIFIED_DATE
                for lead_attribute in lead:
                    if(lead_attribute.tag == 'Logs'):
                        for log_type in lead_attribute:
                            if(log_type.tag == 'CreationLog'):
                                event = log_type.attrib
                                event['Id'] = lead_id
                                event['LogType'] = log_type.tag
                                event['ModifiedAt'] = lead_modified_at
                                events.append(log_type.attrib)
                            else:
                                for log_entry in log_type:
                                    event = log_entry.attrib
                                    event['Id'] = lead_id
                                    event['LogType'] = log_entry.tag
                                    event['ModifiedAt'] = lead_modified_at
                                    events.append(log_entry.attrib)
            
            normalized_events = []
            for event in events:
                normalized_event = { 'account_id': account['id'] }
                # print(event['LogType'].lower())
                # print event_types[event['LogType'].lower()]
                for attribute, propair_field in event_types[event['LogType'].lower()].items():
                    if attribute in event: 
                        normalized_event[ propair_field['field_name' ] ] = sanitize_and_cast( event[ attribute ], propair_field['field_type'] )
                
                normalized_event['account_lead_id']     = sanitize_and_cast( event['Id'], "INT" )
                normalized_event['log_type']            = sanitize_and_cast( event['LogType'], "VARCHAR" )
                normalized_event['modified_at']         = sanitize_and_cast( event['ModifiedAt'], "VARCHAR" )
                #normalized_event['created_at']          = str(datetime.datetime.now())

                if('milestone_id' in normalized_event):
                    try:
                        milestone = (item for item in milestones if item["MilestoneId"] == str(normalized_event['milestone_id'])).next()
                        milestone_name = milestone['MilestoneTitle']
                        normalized_event['milestone_name'] = sanitize_and_cast( milestone_name, "VARCHAR" )
                    except:
                        normalized_event['milestone_name'] = sanitize_and_cast( "NULL", "VARCHAR" )

                normalized_events.append( normalized_event )

            print "::::: TOTAL LEADS PULLED: {} FOR {}".format(len(leads), account['name'])
            print "::::: TOTAL EVENTS TO PROCESS: {} FOR {}".format(len(normalized_events), account['name'])
            #upload_to_s3(account['name'], normalized_events)
            insert_to_db(normalized_events, cur, conn, account)
                
            cur.close()
            conn.close()
        else:
            print "::::: ERROR! ACCOUNT ID {} NOT FOUND".format(account_id)
    except Exception as err:
        stack = traceback.format_exc()
        report_to_rollbar(err, str(stack), str(sys.exc_info()), '')

        

def upload_to_s3(account, record):
    print("::::: Uploading to S3")
    client = boto3.client('s3')
    body = json.dumps(record)[1:-1].replace('}, {', '}\n{')
    client.put_object(Body=body, Bucket=os.environ['BUCKET'], Key="logs/external_events/dt={}/external_events_{}_{}.txt".format(datetime.date.today(), account, time.time()))


def insert_to_db(records, cur, conn, account):
    print("::::: Saving to DB")    
    try:
        if len(records) > 0:
            print("::::: INSERTING ROWS")
            sq = get_insert_statement(records, cur, conn)
            cur.execute(sq)
            count = cur.rowcount
            conn.commit()
            print("::::: TOTAL EVENTS INSERTED: {} FOR {}").format(count, account['name'])
        else:
            print("There isn't anything to insert")

    except Exception as err:
        print("----> DB error: " + str(err))
        print(sq)
        stack = traceback.format_exc()
        report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
        conn.rollback()

def get_insert_statement(records, cur, conn):
    print("::::: Creating insert statement")
    query = "SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_NAME = 'external_events'"
    cur.execute(query)
    data = cur.fetchall()
    query       = 'INSERT INTO external_events('
    values      = "VALUES "
    columns     = []
    ignored_columns = ['id', 'created_at', 'updated_at']
    
    log_types = get_log_types(cur, conn)

    for line in data:
        if line[0] not in ignored_columns:
            columns.append(line[0])

    for i, key in enumerate(columns):
        query += '{}'.format(key)
        if(i < len(columns)-1):
            query += ", "
        else: 
            query += ", created_at, updated_at) ".format(key)


    for i, record in enumerate(records):
        if 'log_type' in record:
            for log in log_types:
                if log[1] == record['log_type']:
                    record['log_type_id'] = log[0]
                    break

        
        values += "("
        date = datetime.datetime.now()

        for x, key in enumerate(columns):
            if key == 'log_subtype_id':
                value = record[key] if key in record else -50
            else:
                value = record[key] if key in record else None
            if(isinstance(value, basestring)):
                value = '$${}$$'.format(value)
            elif value == None:
                value = "NULL" 
                
            values += "{}".format(value)
            
            if(x < len(columns)-1):
                values += ", "
            else:
                values += ", $${}$$, $${}$$)".format(date, date)

        if(i < len(records)-1):
            values += ", "
        
    query  += values
    query = "{} ON CONFLICT (account_id, account_lead_id, log_subtype_id, log_id) DO NOTHING;".format(query)
    
    return query


def get_log_types(cur, conn):
    try:
        print("--------------------- Getting log_types from database")
        sq = "SELECT log_type_id, log_type FROM log_type_lookup"
        cur.execute(sq)
        data = cur.fetchall()
        return data
    except Exception as err:
        print("-----> DB error getting the log_types" + str(err))
        print(sq)
        stack = traceback.format_exc()
        report_to_rollbar(err, str(stack), str(sys.exc_info()), '')

def sanitize_and_cast(val, to_data_type):
    if(val == None):
        return None
    if( "CHAR" in to_data_type ):
        #TODO: Sanitize before casting
        return str(val).replace("'", r"''")
    elif( "INT" in to_data_type):
        #TODO: Sanitize before casting
        if(isinstance(val, basestring)):
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
                        # print e
                        # print val
                        return -1
                else:
                    return -1
        else:
            return val
    else:
        return val


def report_to_rollbar(err, stack, exc_info, account):
    print ":::::::::: GOT AN ERROR TO REPORT :::::::::::::::"

    print(str(err))
    print(stack)
    print(exc_info)

    sns = boto3.client('sns')
    
    py_err = { 'name': str(err), 'message': "EventsDataArchException", 'type': "error", 'trace': {
        'exc_info': "\"{}\"".format( str(exc_info.replace("\"", "'") ) ), 
        'stack': "\"{}\"".format(str(stack).replace("\"", "'"))
        } 
    }
    
    rollbar_error = { 'error': py_err, 'referer': 'Events', 'account': account }
    rollbar_error = json.dumps(rollbar_error)

    response = sns.publish(
        TopicArn=os.environ["TOPIC_ROLLBAR"], 
        Subject="Production Error in Events Lambda",   
        Message=rollbar_error
    )
    print("Response: {}".format(response))