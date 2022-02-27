import os
import pandas as pd 
import psycopg2
import boto3
import json


db_name = os.environ["REDSHIFT_DB_NAME"]
db_host = os.environ["REDSHIFT_ENDPOINT"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = os.environ["REDSHIFT_DB_PORT"]
env = os.environ["ENV"]

#### Getting DB credentials

client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 

db_pass = response['Parameter']['Value']

los_df = pd.read_csv('./functions/data_architecture/encompass/los_propair-bank_20200218.csv', 
    parse_dates=['lead_datetime', 'milestone_date_lock', 'milestone_date_docsigning'], 
    dtype={'account_id': 'Int64', 'account_lead_id': 'Int64', 'lock_yes': 'Int64', 'close_yes': 'Int64', 'match_to_staging': 'Int64'})

initial_len = len(los_df)
los_df = los_df.dropna(subset=['account_id', 'account_lead_id'])
print("::::: Dropped {} null rows".format(len(los_df) - initial_len))

los_df['match_to_staging'] = los_df['match_to_staging'].astype('bool')

match_df = los_df.loc[los_df['match_to_staging'] == True]

print("::::: Processing {} loans where match_to_staging is true".format(len(match_df)))
leads = json.loads(match_df.to_json(orient='records'))

conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
cur = conn.cursor()
stage_table_name = "external_temp"
table_name = "external_lead_details"

sq = 'begin read write;\
		lock {0};\
		create temp table {1} (like {0});'.format(table_name, stage_table_name)

#This will retrieve data insert, update, delete and final insert query
sq += "insert into {}(account_id, account_lead_id, cur_loan_number) values ".format(stage_table_name)

record_values = []
none_count = 0
for i, lead in enumerate(leads):
    account_id = lead['account_id'] if lead['account_id'] != None else None
    lead_id = lead['account_lead_id'] if lead['account_lead_id'] != None else None
    loan_num = lead['cur_loan_number'] if lead['cur_loan_number'] != None else None
    if (not account_id or not lead_id or not loan_num):
        none_count += 1
        continue
    record_values.append("($${}$$::integer, $${}$$::integer, $${}$$::varchar)".format(account_id, lead_id, loan_num))

sq += "{}".format(', '.join(record_values)) + ";"

sq += "update {0} set cur_loan_number={1}.cur_loan_number from {1} where {0}.account_id={1}.account_id and {0}.account_lead_id={1}.account_lead_id;".format(table_name, stage_table_name)

sq += "end transaction;\
drop table {};".format(stage_table_name)

cur.execute(sq)
conn.commit()

print("::::: All Done")

