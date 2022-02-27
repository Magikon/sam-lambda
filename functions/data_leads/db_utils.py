import sys 
import os
import re
import json
import datetime
import psycopg2
import traceback
import dateutil.parser
import datetime
from pplibs.logUtils import secure_log
from time import sleep

column_data = None
column_data_table = None

def remove_duplicates(records, unique_columns):
	seen = set()
	new_records = []
	for d in records:
		h = {k: d[k] if d[k] is not None else -50 for k in unique_columns if k in d}
		h = tuple(sorted(h.items()))
		if h not in seen:
			seen.add(h)
			new_records.append(d)
	return new_records

def get_column_data(table_name, cur, ignored_columns = []):
	global column_data
	global column_data_table
	if (column_data is None or column_data_table != table_name or set(column_data.keys()).intersection(ignored_columns)):
		column_data_table = table_name
		sq = "SELECT \"column\",type from pg_table_def where tablename='{}'".format(table_name)
		count = 0
		successful = False
		while not successful and count < 2:
			try:
				cur.execute(sq)
				data = cur.fetchall()
				if (len(data) > 0):
					successful = True
				else:
					raise Exception("No column data found")
			except Exception as e:
				secure_log("::::: [{0}] Error - {1} trying again...".format(table_name, e))
				secure_log("::::: [{0}] Num of retries: {1}".format(table_name, count))
				sleep(0.5*count)

			count += 1
		
		if (successful):
			secure_log("::::: Found data types")
			column_data = {}
			for line in data:
				if line[0] not in ignored_columns:
					column_data[line[0]] = {"name": line[0], "type": line[1]}

			return column_data
		else:
			raise Exception("[{0}] After {1} retries, was unable to retrieve column_data".format(table_name, count))
	else:
		return column_data

def splitList(array):
    n = len(array)
    half = int(n/2) # py3
    return array[:half+1], array[n-half:]

def build_query(records, stage_table_name, table_name, cur, unique_columns, defaults, mapping, update):
	sq = 'begin read write;\
		lock {0};\
		create temp table {1} (like {0});'.format(table_name, stage_table_name)

	#This will retrieve data insert, update, delete and final insert query
	sq += get_insert_statement(records, stage_table_name, table_name, cur, unique_columns, defaults, mapping, update)

	sq += "end transaction;\
	drop table {};".format(stage_table_name)

	return sq

def build_record_values(record, columns, defaults, mapping, update = None):
	record_values = []
	date = datetime.datetime.now()

	if (len(mapping) > 0):
		for prop in mapping:
			if prop['from'] in record:
				for item in prop['map']:
					if item[1] == record[prop['from']]:
						record[prop['to']] = item[0]
						break
					
	for x, key in enumerate(columns):
		value = record[key] if key in record else None
		data_type = columns[key]['type']
		update_key = '{}='.format(key) if update != None else ''

		if ('timestamp' in data_type and value != None):
			try:
				parse_date = dateutil.parser.parse(value)
				value = parse_date
			except:
				secure_log('[{},{}] Unable to parse datetime for column {}'.format(record['account_id'], record['account_lead_id'], key))
				value = None
		elif('date' in data_type and value != None):
			try:
				parse_date = dateutil.parser.parse(value)
				value = parse_date.strftime('%Y-%m-%d')
			except:
				secure_log('[{},{}] Unable to parse date for column {}'.format(record['account_id'], record['account_lead_id'], key))
				value = None

		if(key in ['updated_at', 'created_at']):
			value = '{}$${}$$'.format(update_key, date)
		elif value == None:
			if (key in defaults):
				value = "{}$${}$$::{}".format(update_key,defaults[key], data_type)
			else:
				value = "{}NULL".format(update_key,key) 
		else:
			value = "{}$${}$$::{}".format(update_key, value, data_type)

		record_values.append(value)
	
	return record_values

def recursive_insert(records, stage_table_name, table_name, cur, conn, unique_columns, defaults, mapping, update, chunk = None):
	successful = False
	count = 0
	while not successful and count < 2:
		try:
			chunk_num = ' Chunk: {} -'.format(chunk) if chunk != None else ''
			secure_log("::::: [{}]{} Data set length: {}".format(table_name, chunk_num, len(records)))
			sq = build_query(records, stage_table_name, table_name, cur, unique_columns, defaults, mapping, update)
			cur.execute(sq)
			conn.commit()
			successful = True
		except Exception as e:
			cur.execute('rollback;')
			conn.commit()
			temp = True
			if (temp==True):
				if(len(records) > 25):
					secure_log("::::: [{}]{} DB ERROR -> Splitting records in half".format(table_name, chunk_num))
					split = splitList(records)
					for i, record_set in enumerate(split):
						recursive_insert(record_set, stage_table_name, table_name, cur, conn, unique_columns, defaults, mapping, update, chunk=('{}-{}'.format(chunk,i) if chunk else i))
				else:
					secure_log("::::: [{}]{} DB ERROR -> Running {} records one by one in order to find bad record".format(table_name, chunk_num, len(records)))
					bad_counter = 0
					for i, item in enumerate(records):
						try:
							sq = build_query([item], stage_table_name, table_name, cur, unique_columns, defaults, mapping, update)
							cur.execute(sq)
							conn.commit()
							secure_log("::::: [{},{}]{} Successfully inserted record".format(table_name, i, chunk_num))
						except Exception as err:
							cur.execute('rollback;')
							conn.commit()
							acc_id = item['account_id'] if 'account_id' in item else 'Unkown'
							secure_log("::::: [{}][Account: {}] Found bad data; Inserting into bad_data table".format(table_name, acc_id))
							secure_log("::::: [{}][Account: {}] {}".format(table_name, acc_id, err.pgerror))
							bad_counter += 1
							dt = datetime.datetime.now()
							bad_sql = """
								INSERT INTO bad_data(created_at, program_name, table_name, table_record, error_message) VALUES 
								($${}$$, $${}$$, $${}$$, $${}$$, $${}$$);
							""".format(dt, 'LeadsConsumer', table_name, re.sub("\$", "/$", json.dumps(item)), re.sub("\$", "/$", err.pgerror))
							cur.execute(bad_sql)
							conn.commit()

					secure_log("::::: [{}]{} Successfully upserted {} records. Failures: {}".format(table_name, chunk_num, len(records)-bad_counter, bad_counter))
				
				return
			else:
				secure_log("::::: [{0}] Error - {1} trying again...".format(table_name, e))
				secure_log("::::: [{0}] Num of retries: {1}".format(table_name, count))
				sleep(0.5*count)

		count += 1
	
	if (successful):
		secure_log("::::: [{0}]{1} Rows upserted".format(table_name, chunk_num))
	else:
		raise Exception("[{0}] After {1} retries upsert was unable to complete".format(table_name, count))


def insert_to_db(records, table_name, cur, conn, unique_columns, defaults={}, mapping=[], update=True):
	try:
		if len(records) > 0:
			secure_log("::::: [{1}] Retrieved {0} records".format(len(records), table_name))	

			unique_records = remove_duplicates(records, unique_columns) 
			secure_log("::::: [{0}] Removed {1} records".format(table_name, len(records)-len(unique_records)))

			if (update):
				secure_log("::::: [{0}] Upserting {1} records".format(table_name, len(unique_records)))
			else:
				secure_log("::::: [{0}] Inserting {1} records".format(table_name, len(unique_records)))

			## - Upsert Procedure:
			# BEGIN
			#1 - Lock events table to prevent serializable errors
			#2 - Create temporary table to store event data
			#3 - Insert event data into temporary table
			#4 - Update external_leads with matched unique events from temporary table
			#5 - Deleted matched unique events from temporary table in order to only leave 'new' events
			#6 - Insert remaining 'new' events from temporary table into external_leads
			# END   

			#Set this to the temporary table name
			stage_table_name = "{}_stage".format(table_name)
		
			recursive_insert(unique_records, stage_table_name, table_name, cur, conn, unique_columns, defaults, mapping, update)

		else:
			secure_log("There isn't anything to insert")

	except Exception as err:
		secure_log("[{}] ----> DB error: ".format(table_name) + str(err))
		conn.rollback()
		#if (os.environ['ENV'] == 'production'):
		#	stack = traceback.format_exc()
		#	report_to_rollbar(err, str(stack), str(sys.exc_info()), '')

def get_insert_statement(records, stage_name, table_name, cur, unique_columns, defaults, mapping, update):

	ignored_columns = ['id']
	columns = get_column_data(table_name, cur, ignored_columns)

	#Query start
	query	   = 'INSERT INTO {0}({1})'.format(stage_name, ', '.join(columns.keys()))
	#This variable will store the insert query for the event data
	values	  = "VALUES "
	#This variable will store the update query for existing events
	update_query = "update {0} set ".format(table_name)
	#This variable will store the delete query removing existing events
	delete_query = "delete from {0} using {1} where ".format(stage_name, table_name)
	#This variable will store the insert query for remaining non-existant events
	insert_query = "insert into {0}".format(table_name)

	update_values = []
	insert_values = []

	for col in columns:
		insert_values.append(col)
		if (col not in unique_columns and col != 'created_at'):
			update_values.append("{0} = {1}.{0}".format(col, stage_name))

	insert_query += "({0}) select {0} from {1};".format(', '.join(insert_values), stage_name)		
	update_query += "{}".format(', '.join(update_values))
	update_query += " from {} where ".format(stage_name)

	for i, uniq in enumerate(unique_columns):
		update_query += "{2}.{0} = {1}.{0}".format(uniq, stage_name, table_name)
		delete_query += "{2}.{0} = {1}.{0}".format(uniq, stage_name, table_name)
		if(i < len(unique_columns)-1):
			update_query += " and "
			delete_query += " and "
		else:
			update_query += ";"
			delete_query += ";"

	for i, record in enumerate(records):
		
		record_values = build_record_values(record, columns, defaults, mapping)
		
		values += "({})".format(', '.join(record_values))
		if(i < len(records)-1):
			values += ", "

	if (update):
		query  += values + '; ' + update_query + " " + delete_query + " " + insert_query
	else:
		query  += values + ';' + " " + delete_query + " " + insert_query

	return query
		

		
def get_unique_agents(leads, account_meta, v_agents, agent_ids = []):
	agents = []
	for lead in leads:
		if 'User' in lead and 'UserId' in lead:
			user_id = int(lead['UserId'])
			cur_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
			agent_data = None
			
			try:
				agent_data = v_agents[user_id]
			except KeyError:
				print("::::: Agent ID: {} not found in Velocify!".format(user_id))

			agent = {
				"account_id": account_meta['account_id'],
				"agent_id": user_id,
				"name_velocify": lead['User'],
				"email": agent_data['AgentEmail'] if agent_data else '',
				"primary_phone": agent_data['PhoneWork'] if agent_data else '', 
				"created_at": cur_date, 
				"updated_at": cur_date
			}

			try:
				agent['first_name'] = agent['name_velocify'].split(', ')[1]
				agent['last_name'] = agent['name_velocify'].split(', ')[0]
				agent['name'] = '{} {}'.format(agent['first_name'],agent['last_name'])
			except:
				agent['name'] = None
				agent['first_name'] = None
				agent['last_name'] = None 
				
			if user_id not in agent_ids:
				agent_ids.append(user_id)
				agents.append(agent)

	return agents