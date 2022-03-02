from time import sleep
import datetime
import dateutil.parser
import re
import pandas as pd

def run_transaction(cur, conn, sql, retries=2):
	successfull = False
	count = 0
	error = None
	while successfull == False and count < retries:
		try:
			cur.execute(sql)
			conn.commit()
			successfull = True
		except Exception as err:
			error = err
			print("::::: DB ERROR -> {}. Trying again.. count: {}".format(err, count))
			count += 1
			conn.rollback()
			
	if (successfull):
		print(":::: Transaction Complete")
	else:	
		print("----> DB error. After {} retries was unable to complete: ".format(retries))
		raise Exception(error)

def get_column_data(table_name, cur):
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
			print("::::: [{0}] Error - {1} trying again...".format(table_name, e))
			print("::::: [{0}] Num of retries: {1}".format(table_name, count))
			sleep(0.5 * count)

		count += 1

	if (successful):
		print("::::: Found data types")
		column_data = {}
		for line in data:
			column_data[line[0]] = {"name": line[0], "type": line[1]}

		return column_data
	else:
		raise Exception("[{0}] After {1} retries, was unable to retrieve column_data".format(table_name, count))


def build_record_values(record, columns):
	record_values = []

	for x, key in enumerate(columns):
		value = record[key] if key in record else None
		data_type = columns[key]['type']

		if 'timestamp' in data_type and value is not None:
			try:
				parse_date = dateutil.parser.parse(value)
				value = parse_date
			except:
				print('Unable to parse timestamp for column {}'.format(key))
				value = None
		elif 'date' in data_type and value is not None:
			try:
				parse_date = dateutil.parser.parse(value)
				value = parse_date.strftime('%Y-%m-%d')
			except:
				print('Unable to parse date for column {}'.format(key))
				value = None

		if value is None or value == '':
			value = "NULL"
		else:
			value = "$${}$$::{}".format(value, data_type)

		record_values.append(value)

	return record_values

def sanitize_and_cast(val, to_data_type, field, accepted_fields):
	if (val == None):
		return None
	elif ("date" in to_data_type.lower() or "time" in to_data_type.lower()):
		### Check if string is a malformed date ###
		# print( "DATE DEBUGGER")
		try:

			match = re.match(r"(^20\d{2}$)", val)  # 2005, 2015, 2021, etc
			if match and match.groups() and len(match.groups()) == 1:
				print("Formatted to: {}-01-01".format(val))
				return "{}-01-01".format(val)

			match = re.match(r"(1/0/1900)", val)  # 1/0/1900
			if match and match.groups() and len(match.groups()) == 1:
				print("Formatted to: 1900-01-01")
				return "1900-01-01"

			match = re.match(r"(0\d{1}|1[0-2])([0-2]\d{1}|3[0-1])(20\d{2})", val)  # 06012012
			if match and match.groups() and len(match.groups()) == 3:
				print("Formatted to: {}-{}-{}".format(match.groups()[2], match.groups()[0], match.groups()[1]))
				return "{}-{}-{}".format(match.groups()[2], match.groups()[0], match.groups()[1])

		except Exception as e:
			return val

		return val
	elif ("char" in to_data_type.lower()):
		# TODO: Sanitize before casting
		try:
			if (str(val).upper() == "NAN" or str(val).upper() == "NA" or str(val).upper() == "None"):
				return None
			else:
				return re.sub('\$\$', '', str(val).encode('ascii', 'ignore').decode('unicode_escape'))

		except Exception as e:
			print("::::: String sanitization error! Setting to blank. Field: {}, Value: {}".format(field, val))
			print(e)
			return ''

	elif ("int" in to_data_type.lower()):
		# TODO: Sanitize before casting
		if (isinstance(val, str)):
			if (val == 'True' or val.upper() == 'YES' or val.upper() == 'ACTIVE'):
				return 1
			elif (val == 'False' or val.upper() == 'NO' or val.upper() == 'TERMINATED'):
				return 0
			else:
				numvalue = None
				orig_val = val

				if 'Down' not in orig_val:
					numvalue = re.sub("[^eE0-9.+-]", "", str(val))
				else:
					print('ERROR: (DOWN in val): field,orig_val,val = {},{},{}'.format(field, orig_val, val))
					numvalue = None

				if (numvalue is not None):
					try:
						if field in accepted_fields:
							x = float(re.sub("[^\d]", "", str(numvalue)))
							if int(x) > 100000000:
								print('(SPLITTING DATA) field, value = {}, {}'.format(field, numvalue))
								numvalue = ((int(str(int(x))[0:len(str(int(x))) // 2]) + int(
									str(int(x))[len(str(int(x))) // 2:len(str(int(x)))])) // 2)
								print('(POST SPLIT): {}'.format(numvalue))
							else:
								numvalue = x
						else:
							numvalue = re.sub("(?<!^)[+-]", "", str(numvalue))
					except Exception as e:
						print(
							'ERROR (numvalue): field, orig_val, numvalue = {},{},{} '.format(field, orig_val, numvalue))
						print(e)

				if (numvalue is not None):
					try:
						final_val = int(float(numvalue))

						# If phone numer is greater than 10 digits 
						# Set to -2. Identifying it as a bad phone number
						if "phone" in field and final_val > 100000000000:
							final_val = -2

						return final_val
					except Exception as e:
						print('ERROR (numvalue is not None): {},{}'.format(val, e))
						return -1
				else:
					print('ERROR: numvalue is None')
					print("Could not parse {} to {}.".format(val, to_data_type))
					return -1
		else:
			return val
	elif("float" in to_data_type.lower() or "double" in to_data_type.lower()):
		#TODO: Sanitize before casting
		try:
			return_val = float(val)
		except:
			try:
				return_val = float(re.sub("[^0-9.-]", "", str(val)))
			except:
				return_val = None
		
		return return_val
	else:
		return val