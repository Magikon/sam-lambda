import os
import sys
import json
import boto3
import io
import psycopg2
import re
import pandas as pd
import pydash as _
import traceback
import datetime
import dateutil.parser
import requests
from io import StringIO

from time import sleep

from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.db_utils import sanitize_and_cast, get_column_data, build_record_values, run_transaction
from pplibs.customErrors import ProPairError, report_to_rollbar

pd.set_option('display.max_columns', None)
# Getting DB credentials
client = boto3.client('ssm')
wd = boto3.client('workdocs', region_name='us-west-2')

response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]),
	WithDecryption=True
)
db_pass = response['Parameter']['Value']
##### Redshift env variables
db_host = os.environ["REDSHIFT_ENDPOINT"]
db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = 5439

pd.set_option('display.width', 2000)
pd.set_option('display.max_colwidth', 200)
pd.set_option('display.max_rows', 1000)

dynamodb = boto3.resource('dynamodb')
client = boto3.client('ssm')

rollbar_topic = os.environ['TOPIC_ROLLBAR']

def build_transaction(records, column_data):
	stage_name = 'temp_agents'

	# Query start
	sql = """
		begin read write;
		create temp table {0}
			(like agent_profiles);
		""".format(stage_name)

	sql += 'INSERT INTO {0}({1})'.format(stage_name, ', '.join(column_data.keys()))
	values = "VALUES "

	for i, record in enumerate(records):
		record_values = build_record_values(record, column_data)

		values += "({})".format(', '.join(record_values))
		if i < len(records) - 1:
			values += ", "

	sql += values + ';'

	uniq = ['id', 'agent_id', 'account_id']
	update_query = "update agent_profiles set "
	update_values = []

	for col in column_data:
		if col not in uniq and col != 'created_at':
			update_values.append("{0} = {1}.{0}".format(col, stage_name))

	update_query += "{}".format(', '.join(update_values))
	update_query += " from {} where ".format(stage_name)

	for i, item in enumerate(uniq):
		update_query += "{2}.{0} = {1}.{0}".format(item, stage_name, 'agent_profiles')
		if i < len(uniq) - 1:
			update_query += " and "
		else:
			update_query += ";"

	sql += update_query
	sql += f"""
		end transaction;\
		drop table {stage_name};
		"""
		

	return sql

def get_global_attr(cur, table_name, account_id):
	secure_log("::::: Fetching Global Attribute Mapping Info")

	attribute_map = []

	try:
		query = "SELECT propair_field, customer_field_name, customer_original_field_name, customer_split, datatype FROM " \
				"global_attribute_lookup WHERE account_id={} AND table_name='{}'".format(account_id, table_name)

		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			attribute_map.append(
				{'propair': line[0], 'customer': line[1].upper().replace(" ", "") if line[1] is not None else None, 'original': line[2], 'split': line[3],
				'datatype': line[4]})
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), account_id)
		secure_log("I am unable to connect to the database")

	if (len(attribute_map) == 0):
		raise Exception("No data in global attribute map!")
	else:
		return attribute_map

def update_lookup(csv_string, file_meta, folder_id):

	secure_log("::::: Initiating Document Version Upload")
	update_meta = wd.initiate_document_version_upload(
		Id=file_meta['Id'],
		Name=file_meta['LatestVersionMetadata']['Name'],
		ParentFolderId=folder_id,
		ContentType='text/csv'
	)

	secure_log("::::: Uploading File")
	r = requests.put(update_meta['UploadMetadata']['UploadUrl'], data=csv_string, headers=update_meta['UploadMetadata']['SignedHeaders'])

	secure_log("::::: Activating File")
	r = wd.update_document_version(
		DocumentId=update_meta['Metadata']['Id'],
		VersionId=update_meta['Metadata']['LatestVersionMetadata']['Id'],
		VersionStatus='ACTIVE'
	)

	secure_log("::::: File Successfully Updated")
	
def get_lookup(folder_id, file_name):
	
	secure_log("::::: Fetching role lookup csv")
	response = wd.describe_folder_contents(FolderId=folder_id)

	file_meta = next((x for x in response['Documents'] if file_name in x['LatestVersionMetadata']['Name']),  None)

	if file_meta:
		res = wd.get_document_version(DocumentId=file_meta['Id'], VersionId=file_meta['LatestVersionMetadata']['Id'], Fields='SOURCE')

		file_data = requests.get(res['Metadata']['Source']['ORIGINAL'])

		return file_data.content, file_meta
	else:
		raise Exception("Lookups File not found: {}".format(file_name))

	
def update(event, context):
	try:
		secure_log("::::: Initializing HRUpdates Function")

		secure_log("::::: Connecting to database..")
		conn = psycopg2.connect(
			"dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()

		secure_log(event['Records'][0]['s3'])
		key = event['Records'][0]['s3']['object']['key']
		key = re.sub('\+', ' ', key)
		bucket = event['Records'][0]['s3']['bucket']['name']

		file_name_pieces = key.split('/')
		account_name = file_name_pieces[-2]
		account_id = None
		file_name = file_name_pieces[-1]
		secure_log("::::: File Name: {}".format(file_name))

		secure_log("::::: Fetching Account Info")
		sq = "SELECT id FROM accounts WHERE name = '{}';".format(account_name.lower())
		cur.execute(sq)
		data = cur.fetchall()
		for account_row in data:
			account_id = account_row[0]

		account_config = get_account_config(account_id)

		secure_log("::::: Downloading file...")
		s3 = boto3.client('s3')
		obj = s3.get_object(Bucket=bucket, Key=key)
		update_df = pd.read_csv(io.BytesIO(obj['Body'].read()))
		update_df = update_df.convert_dtypes()

		secure_log("::::: Retrieved {} agents from file".format(len(update_df)))


		attribute_map = get_global_attr(cur, 'agent_profiles', account_id)

		column_data = get_column_data('agent_profiles', cur)
		
		# Check if fields aren't mapped
		if any([x['customer'] in [str(y).upper().replace(" ", "") for y in update_df.columns] for x in attribute_map]):

			# Standardize df columns
			update_df = update_df.rename(columns={x: x.upper().replace(" ", "") for x in update_df.columns})
			
			# Removing extra columns
			secure_log("::::: Mapping and sanitizing...")
			update_df = update_df.drop(
				columns=[x for x in update_df.columns if x not in [y['customer'] for y in attribute_map]])

			# Set account_id
			update_df['account_id'] = account_id

			for attribute in attribute_map:
				if attribute['customer'] in update_df.columns:
					update_df = update_df.rename(columns={attribute['customer']: attribute['propair']})

					datatype = column_data[attribute['propair']]['type'] if attribute['propair'] in column_data else attribute[
						'datatype']

					# Sanitize data
					update_df[attribute['propair']] = update_df[attribute['propair']].apply(
						lambda x: sanitize_and_cast(x, datatype, attribute['propair'], []))

					if 'INT' in str(datatype).upper():
						update_df = update_df.astype({attribute['propair']: 'Int64'}, errors='ignore')
						# Set to float / make < sys.maxsize to avoid 'Python int too large to convert to C long' error
						# update_df = update_df.astype({attribute['propair']: 'float'}, errors='ignore')
						# update_df[attribute['propair']] = [
						#    None if pd.isnull(x) == True else sys.maxsize - 1 if x >= sys.maxsize else int(x) for x in
						#    update_df[attribute['propair']]]
					elif 'DATE' in str(datatype).upper() or 'TIME' in str(datatype).upper():
						update_df[attribute['propair']] = pd.to_datetime(update_df[attribute['propair']], errors='ignore')

		secure_log("::::: Downloading Role Lookup file...")

		# TODO Find a better place for this
		if os.environ['ENV'] == 'staging':	
			folder_id = 'af9da87e8e62305bea21475cbc9ab1e69b509df94b1bb748c626b8127629966b'
		elif os.environ['ENV'] == 'production':
			folder_id = '1072fafff0376683910670c07f0d267957d1bba6e0e3ea13af7a95f980d10cf1'

		file_content, file_meta = get_lookup(folder_id, 'role_lookup.csv')
		lookup_df = pd.read_csv(io.BytesIO(file_content))
		lookup_df = lookup_df.convert_dtypes()
		lookup_df = lookup_df[(pd.isna(lookup_df.title) == False) & (lookup_df.title != 'undefined')]
		
		print(update_df.dtypes)
		print(lookup_df.dtypes)
		missing_roles = update_df[update_df['title'].isin(lookup_df['title'].unique()) == False]['title'].unique()

		missing_roles = [x for x in missing_roles if pd.isna(x) == False and x != '<NA>']
		# Upload any missing Titles in Lookup
		if (len(missing_roles) > 0):
			secure_log(":::: Found new Titles! Uploading to Workdocs")
			
			new_lookup_df = lookup_df
			for i in missing_roles:
				new_lookup_df = new_lookup_df.append({'title': i}, ignore_index=True)

			csv_buffer = new_lookup_df[['title', 'role']].to_csv(header=['title', 'role'], index=False)
			update_lookup(csv_buffer, file_meta, folder_id)

		# Remove missing roles
		lookup_df = lookup_df[(pd.isna(lookup_df.role) == False) & (lookup_df.role != 'undefined')]

		# Fetching current agents
		secure_log("::::: Fetching current agents...")
		cur.execute(
			f"SELECT * from agent_profiles where account_id in ({','.join([str(x) for x in update_df['account_id'].unique()])})")
		orig_df = pd.DataFrame(cur.fetchall(), columns=[desc[0] for desc in cur.description])
		orig_df = orig_df.convert_dtypes()
		orig_df = orig_df.sort_values(by=['id']).reset_index(drop=True)
		secure_log("::::: Retrieved {} agents from DB".format(len(orig_df)))

		update_df['email_MATCH'] = update_df['email'].str.upper().str.strip()
		orig_df['email_MATCH'] = orig_df['email'].str.upper().str.strip()

		# Set up Role Matching
		if 'title' in update_df.columns:
			update_df['title_MATCH'] = update_df.title.str.upper().str.replace(' ', '')
			lookup_df['title_MATCH'] = lookup_df.title.str.upper().str.replace(' ', '')

			if 'role' in update_df.columns:
				update_df = update_df.drop(columns=['role'])

			update_df = update_df.merge(lookup_df, how='left', on='title_MATCH')
			
		# If first_name or last_name are missing try to extract values from name_velocify 
		# Finally, if name_velocify is missing then raise Error
		
		if 'first_name' not in update_df.columns and 'last_name' not in update_df.columns:
			if 'name' in update_df.columns:
				update_df['first_name'] = [x.strip().split(' ')[0] if pd.isna(x) == False else pd.NA for x in update_df['name']]
				update_df['last_name'] = [x.strip().split(' ')[-1] if pd.isna(x) == False else pd.NA for x in update_df['name']]
			elif 'name_velocify' in update_df.columns:
				update_df['first_name'] = [x.split(',')[-1].strip() if pd.isna(x) == False else pd.NA for x in update_df['name_velocify']]
				update_df['last_name'] = [x.split(',')[0].strip() if pd.isna(x) == False else pd.NA for x in update_df['name_velocify']]
				update_df['name'] = update_df['first_name'] + ' ' + update_df['last_name']
			else:
				raise Exception("Invalid File; Missing agent names to match on!")
		else:
			update_df['name'] = update_df['first_name'] + ' ' + update_df['last_name']
			
		# Set name_MATCH 
		update_df['name_MATCH'] = (update_df['last_name'] + update_df['first_name']).str.replace('[, ]', '').str.upper()
		orig_df['name_MATCH'] = orig_df['name_velocify'].str.replace('[, ]', '').str.upper()

		# Check for Agent ID. Or set to None
		orig_df['id_MATCH'] = orig_df['agent_id']
		if 'agent_id' not in update_df.columns:
			update_df['id_MATCH'] = -1
		else:
			update_df['id_MATCH'] = update_df['agent_id']
		
		update_df['id_MATCH'] = update_df['id_MATCH'].astype('Int64')

		secure_log("::::: Merging results...")
		# Merge Dataframes on email and name
		merged_df = pd.concat([
			orig_df[pd.isnull(orig_df.agent_id) == False].merge(update_df, how='inner', on='id_MATCH').reset_index(drop=True),
			orig_df[pd.isnull(orig_df.email_MATCH) == False].merge(update_df[pd.isnull(update_df.email_MATCH) == False],
																how='inner', on='email_MATCH').reset_index(drop=True),
			orig_df[pd.isnull(orig_df.name_MATCH) == False].merge(update_df[pd.isnull(update_df.name_MATCH) == False], how='inner', on='name_MATCH').reset_index(drop=True)
		]
		)

		try:
			immutable = account_config['hr_updates']['immutable_columns']
		except KeyError:
			immutable = [
		      "recommend",
		      "email",
		      "rec_type",
		      "created_at",
		      "name_velocify",
		      "first_name",
		      "last_name",
		      "name",
			  "agent_id"
		    ]
		
		secure_log("::::: Immutable columns: ", immutable)
		
		# Remove duplicate columns
		for col in column_data:
			field = col
			dtype = column_data[col]['type']

			if f"{field}_x" in merged_df.columns:

				# If field is immutable.. prioritize left join (original value)
				# Otherwise, prioritize right join (hr file value)
				if field in immutable:
					merged_df[f'{field}'] = merged_df[f'{field}_x'].where(merged_df[f'{field}_x'].notnull(),
																		merged_df[f'{field}_y'].astype(
																			merged_df[f'{field}_x'].dtype))
				else:
					merged_df[f'{field}'] = merged_df[f'{field}_y'].where(merged_df[f'{field}_y'].notnull(),
																		merged_df[f'{field}_x'].astype(
																			merged_df[f'{field}_y'].dtype))															

				merged_df = merged_df.drop(columns=[f'{field}_x', f'{field}_y'])

		# Remove duplicate rows
		final_df = merged_df.sort_values(by=['id_MATCH', 'email_MATCH'], na_position='last').drop_duplicates(subset=["id", "account_id", "agent_id"]).reset_index(
			drop=True)

		secure_log("::::: Dropping empty rows. Current Length: {}".format(len(final_df)))
		final_df = final_df[(pd.isnull(final_df.agent_id) == False) & (pd.isnull(final_df.account_id) == False) & (
				pd.isnull(final_df.id) == False)].reset_index(drop=True)
		secure_log("::::: Dropped empty rows. Current Length: {}".format(len(final_df)))
	
		
		secure_log("::::: Dropping extra columns...")
		final_df = final_df.drop(
			columns=[x for x in final_df.columns if x not in column_data.keys()]).reset_index(
			drop=True)

		
		final_df['updated_at'] = datetime.datetime.now()

		df_json = json.loads(final_df.to_json(orient='records', date_format='iso'))
		
		
		secure_log("::::: Running Updates on {} agents".format(len(final_df)))
		sql = build_transaction(df_json, {x['name']: x for x in column_data.values() if x['name'] in final_df.columns})
		run_transaction(cur, conn, sql)


		secure_log("::::: Done")
		
	except Exception as err:
		secure_log("::::: ERROR ", err)
		secure_log(traceback.format_exc())

		py_err = ProPairError(err, "HRUpdates", exec_info=sys.exc_info(), stack=traceback.format_exc())
		try:
			py_err.account = account_name
			report_to_rollbar(rollbar_topic, py_err)
			conn.rollback()
		except Exception as err:
			py_err.account = "NO_ACCOUNT"
			report_to_rollbar(rollbar_topic, py_err)
			conn.rollback()
