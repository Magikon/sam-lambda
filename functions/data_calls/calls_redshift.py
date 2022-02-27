import sys
import os
import traceback

sys.path.insert(0, './lib/idna')
sys.path.insert(0, './lib/python-certifi')
sys.path.insert(0, './lib/chardet')
sys.path.insert(0, './lib/urllib3')
sys.path.insert(0, './lib/requests')
sys.path.insert(0, './lib/pytz')
sys.path.insert(0, './lib')


import urllib3
import requests
import xml.etree.ElementTree as ET
import datetime
from datetime import datetime, timedelta, date
import re
import pytz
import psycopg2
import boto3
import json

from generate_calls import generate
from pplibs.logUtils import secure_log
from pplibs.logUtils import get_account_config

velocify_get_report_url = 'https://service.leads360.com/ClientService.asmx/GetCallHistoryReport'

DB_NAME = os.environ["REDSHIFT_DB_NAME"]
DB_USER = os.environ["REDSHIFT_DB_USER"]

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
    Name="/{}/redshift/master-database-password".format(os.environ["ENV"]), 
    WithDecryption=True
) 

DB_PASS = response['Parameter']['Value']
DB_HOST = os.environ["REDSHIFT_ENDPOINT"]
DB_PORT = os.environ["REDSHIFT_DB_PORT"]

def daterange(start_date, end_date):
    for n in range(int ((end_date - start_date).days)):
        yield start_date + timedelta(n)

def cast(value, field):
    cast_value = value
    
    if field == 'call_duration' or field == 'wait_time':
        if value == '' or value == None:
            cast_value = '00:00'
        else:
            cast_value = "00:{}".format(value) if len(value.split(":")) == 1 else value

            dm, ds = cast_value.split(':')
            cast_value = int(dm) * 60 + int(ds)

    if field == 'account_lead_id':
        cast_value = int(value) if value.isdigit() else None

    return cast_value

def get_record_dict( child, account_id, mapping):

    call_record = child.attrib

    normalized_call_record = {}
    for attribute in mapping:
        if attribute['original'] in call_record:
            normalized_call_record[ attribute['propair'] ] = cast(call_record[ attribute['original'] ], attribute['propair'])
        elif attribute['customer'] in call_record: 
            normalized_call_record[ attribute['propair'] ] = cast(call_record[ attribute['customer'] ], attribute['propair'])  

    normalized_call_record['account_id'] = account_id
    
    return normalized_call_record

def get_char_fields(cur, table):
    print("Find data types")
    sq = "SELECT \"column\",type from pg_table_def where tablename='{}'".format(table)
    cur.execute(sq)
    column_data = cur.fetchall()

    char_column_dict = {}
    column_data = [x for x in column_data if 'character' in x[1]]
    char_column_dict = dict(zip([x[0] for x in column_data],["::char(" + x[1].split('(')[1].split(')')[0] + ")" for x in column_data]))

    print('::::: Current char columns: {}'.format(char_column_dict))
    return char_column_dict


def insert_records_to_db( records, stored_procedure ):

    #conn = psycopg2.connect('host={} dbname={} user={} password={}'.format(DB_HOST, DB_NAME, DB_USER, DB_PASS))
    conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASS))
    cur = conn.cursor()

    char_column_dict = get_char_fields(cur, 'call_logs_upsert')

    inserted = 0

    for record in records:
        query  = "INSERT INTO call_logs_upsert("
        values = " VALUES("
        for idx, key in enumerate(record):
            value = record[key]

            if idx < len(record)-1:
                query += "{}, ".format(key)

                if(os.environ['ENV'] == 'staging' and key == 'call_id'):
                    values += "(SELECT coalesce(max(call_id), 0) + 1 from call_logs WHERE account_id = {}), ".format(record['account_id'])
                elif(isinstance(value, str)):
                    try:
                        value = '$${}$${}'.format(value, char_column_dict[key])
                    except:
                        value = '$${}$$'.format(value)

                elif value == None:
                    values += "NULL, "
                else:
                    values += "{}, ".format(value)
            else:
                query += "{}, created_at, updated_at) ".format(key)
                cur_timestamp = datetime.now()
                if(isinstance(value, str)):
                    values += "$${}$$, $${}$$, $${}$$)".format(value.replace("'", "''"), cur_timestamp, cur_timestamp)
                elif value == None:
                    values += "NULL, $${}$$, $${}$$)".format(cur_timestamp, cur_timestamp)
                else:
                    values += "{}, $${}$$, $${}$$)".format(value, cur_timestamp, cur_timestamp)
        
        query  += values
        # query  += " ON CONFLICT (call_id) DO NOTHING;"

        sp_increment = "call {}()".format(stored_procedure)

        try:
            cur.execute(query)
            cur.execute(sp_increment)            
            conn.commit()
            inserted += 1
        except Exception as err:
            secure_log(err)
            conn.rollback()
    
    cur.close()
    conn.close()

    secure_log("Total of {} records inserted in DB.".format( inserted ) )

def update(event, context):

    account_meta = json.loads(event['Records'][0]['Sns']['Message'])
        
    account_id          = account_meta['account_id']
    account             = account_meta['account']
    #velocify_username   = account_meta['velocify_username']
    #velocify_password   = account_meta['velocify_password']
    records             = []

    get_account_config(account_id)
    secure_log("Initializing Data Architecture Calls")


    # fetching global_attribute
    conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASS))
    cur = conn.cursor()
    query = "SELECT propair_field, customer_field_name, customer_original_field_name, datatype FROM global_attribute_lookup\
    WHERE account_id = '{0}' AND table_name='call_log' AND ( customer_field_name IS NOT NULL OR customer_original_field_name IS NOT NULL);".format(account_id)
    cur.execute(query)
    data = cur.fetchall()

    call_map = []
    
    for line in data:
        call_map.append( { 'propair': line[0], 'customer':line[1], 'original':line[2], 'datatype': line[3] } )

    if (os.environ['ENV'] == 'staging'):
        data = generate()

        for child in data:
            record = get_record_dict( child, account_id, call_map )
            records.append( record )

    else:
        payload = {'username':velocify_username, 'password': velocify_password}
        
        #Grab last event timestamp
        #conn = psycopg2.connect('host={} dbname={} user={} password={}'.format(DB_HOST, DB_NAME, DB_USER, DB_PASS))
        conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(DB_NAME, DB_HOST, DB_PORT, DB_USER, DB_PASS))
        cur = conn.cursor()
        query = "SELECT max(call_timestamp) as call_timestamp FROM call_logs WHERE account_id={};".format(account_id)
        cur.execute(query)
        data = cur.fetchall()
        cur.close()
        conn.close()

        if( len(data) > 0 and len( data[0] ) > 0 and data[0][0] != None ): 
            start_date = data[0][0]
        else:
            start_date = datetime(2017, 1, 1, 0, 0, 0, 0)

        hours       = 8
        end_date    = start_date + timedelta(hours=hours)

        while( len(records) < 1000 and end_date <= (datetime.today() + timedelta(hours=12)) ):
            end_date                = start_date + timedelta(hours=hours)
            payload['startDate']    = start_date.strftime("%m/%d/%Y %I:%M:%S %p")
            payload['endDate']      = end_date.strftime("%m/%d/%Y %I:%M:%S %p")

            secure_log("Fetching data from {} to {}".format(payload['startDate'], payload['endDate']) )
            
            r = requests.post(velocify_get_report_url, headers={'Content-Type': 'application/x-www-form-urlencoded'}, data=payload, timeout=None)
            data = ET.fromstring(test.encode('ascii', 'ignore'))

            for child in data:
                record = get_record_dict( child, account_id, call_map )
                records.append( record )
            
            start_date = end_date - timedelta(hours=1)

    
    secure_log("Total of {} records fetched.".format(len(records)))
    stored_procedure = "sp_call_logs_increment"
    
    insert_records_to_db( records, stored_procedure )
