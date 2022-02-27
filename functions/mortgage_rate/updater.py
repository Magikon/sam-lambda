import numpy as np
import pandas as pd
import datetime as dt
import requests
import xlrd
import psycopg2 as postdb
import os
import sys
import boto3
import json
import traceback
import sys
from pplibs.logUtils import secure_log

pd.set_option('display.max_rows',400)
pd.set_option('display.width',1000)
pd.set_option('display.max_colwidth',200)
pd.set_option('display.max_rows',400)

####################################
####################################
############ FUNCTIONS #############  
####################################
####################################
headers=['mortgage_rate_date','us_30yr_rate','us_30yr_pts','us_15yr_rate','us_15yr_pts','us_5arm_rate','us_5arm_pts','us_5arm_mar']
def get_rates():
	#year = ['2013','2014','2015','2016','2017','2018','2019']

	today_year = (dt.datetime.today()).year

	out_data = []
	url = "http://www.freddiemac.com/pmms/docs/historicalweeklydata.xls".format(today_year)

	try:
		r = requests.get(url)

		##############################
		# Grab data from current year
		##############################
		xfile = xlrd.open_workbook(file_contents=r.content)
		sheet_1pmms = pd.read_excel(xfile, sheet_name='1PMMS{}'.format(today_year))
		sheet_1pmms = sheet_1pmms.fillna('')

		onepmms_rows = []
		for rownum in range(sheet_1pmms.shape[0]):
			onepmms_rows.append(sheet_1pmms.iloc[rownum, :])


		for cur_row in range(6,99):
			try:
				row = onepmms_rows[cur_row].values.tolist()
				if (row[0] != ''):
					out_data.append(row)
			except Exception as e:
				pass
		print("1PPMSYYYY Sheet processed")
		##############################
		# Grab data from prior years
		##############################
		sheet_full_history = pd.read_excel(xfile, sheet_name='Full History')
		sheet_full_history = sheet_full_history.fillna('')

		full_history_rows = []
		for rownum in range(sheet_full_history.shape[0]):
			full_history_rows.append(sheet_full_history.iloc[rownum, :])
		
		for cur_row in range(6, 2999):
			try:
				row = full_history_rows[cur_row].values.tolist()
				if (row[0] != '' and not pd.isnull(row[0])):
					out_data.append(row)
			except Exception as e:
				pass
		print("Full History Sheet processed")
	except Exception as e:
		print('year={} . {}'.format(today_year,e))

	output_df = pd.DataFrame(out_data)
	output_df = output_df.rename(columns={col:x for col,x in zip(output_df.columns,headers)})

	output_df = output_df.sort_values('mortgage_rate_date').reset_index(drop=True)

	return output_df

def next_weekday(d, weekday):
	days_ahead = weekday - d.weekday()
	if days_ahead <= 0: # Target day already happened this week
		days_ahead += 7
	return d + dt.timedelta(days_ahead)


def update(event, context):

	#### Getting DB credentials
	client = boto3.client('ssm')
	response = client.get_parameter(
		Name="/{}/redshift/master-database-password".format(os.environ["ENV"]),
		WithDecryption=True
	)

	db_name = os.environ["REDSHIFT_DB_NAME"]
	db_user = os.environ["REDSHIFT_DB_USER"]
	db_port = os.environ["REDSHIFT_DB_PORT"]
	db_host = os.environ["REDSHIFT_ENDPOINT"]
	db_pass = response['Parameter']['Value']

	mortgage_df = get_rates()

	print('Add additional metrics')
	mortgage_df.loc[:,'us_30yr_1wkdelta'] = mortgage_df['us_30yr_rate'].shift(1)
	mortgage_df.loc[:,'us_30yr_4wkdelta'] = mortgage_df['us_30yr_rate'].shift(4)
	mortgage_df.loc[:,'us_30yr_rate_1wkdelta'] = [None if (y == '') | (pd.isnull(y) == True) else (float(x)-float(y))/float(y) \
											for x,y in zip(mortgage_df['us_30yr_rate'],mortgage_df['us_30yr_1wkdelta'])]
	mortgage_df.loc[:,'us_30yr_rate_4wkdelta'] = [None if (y == '') | (pd.isnull(y) == True) else (float(x)-float(y))/float(y) \
											for x,y in zip(mortgage_df['us_30yr_rate'],mortgage_df['us_30yr_4wkdelta'])]
	
	mortgage_df.loc[:,'us_15yr_1wkdelta'] = mortgage_df['us_15yr_rate'].shift(1)
	mortgage_df.loc[:,'us_15yr_4wkdelta'] = mortgage_df['us_15yr_rate'].shift(4)
	mortgage_df.loc[:,'us_15yr_rate_1wkdelta'] = [None if (y == '') | (pd.isnull(y) == True) else (float(x)-float(y))/float(y) \
											for x,y in zip(mortgage_df['us_15yr_rate'],mortgage_df['us_15yr_1wkdelta'])]
	mortgage_df.loc[:,'us_15yr_rate_4wkdelta'] = [None if (y == '') | (pd.isnull(y) == True)else (float(x)-float(y))/float(y) \
											for x,y in zip(mortgage_df['us_15yr_rate'],mortgage_df['us_15yr_4wkdelta'])]
	
	mortgage_df.loc[:,'us_5arm_1wkdelta'] = mortgage_df['us_5arm_rate'].shift(1)
	mortgage_df.loc[:,'us_5arm_4wkdelta'] = mortgage_df['us_5arm_rate'].shift(4)
	mortgage_df.loc[:,'us_5arm_rate_1wkdelta'] = [None if (y == '') | (pd.isnull(y) == True) else (float(x)-float(y))/float(y) \
											for x,y in zip(mortgage_df['us_5arm_rate'],mortgage_df['us_5arm_1wkdelta'])]
	mortgage_df.loc[:,'us_5arm_rate_4wkdelta'] = [None if (y == '') | (pd.isnull(y) == True) else (float(x)-float(y))/float(y) \
											for x,y in zip(mortgage_df['us_5arm_rate'],mortgage_df['us_5arm_4wkdelta'])]
	
	# Find next thursday
	next_thursday = next_weekday(dt.datetime.now().date(),3)
	
	# Reindex to add all days
	idx = pd.date_range(min(mortgage_df['mortgage_rate_date']),next_thursday)
	mortgage_df = mortgage_df.set_index(['mortgage_rate_date'])
	
	# Fill in days
	mortgage_df = mortgage_df.reindex(idx,fill_value=None).ffill().reset_index().rename(columns={'index':'mortgage_rate_date'})

	# Add created_at
	mortgage_df = pd.merge(pd.DataFrame([dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")] * len(mortgage_df))\
														.rename(columns={0:'created_at'}),mortgage_df,\
														'left',left_index=True,right_index=True)

	# Output to csv
	#################################
	# Move mortgage data to REDSHIFT 
	#################################
	statusCode = 0
	message = ''	
	try:
		rs_con = postdb.connect('host={} dbname={} port={} user={} password={}'.format(db_host, db_name, db_port, db_user, db_pass))
		rs_cur = rs_con.cursor()

		rs_query = 'TRUNCATE mortgage_rate'
		rs_cur.execute(rs_query)
		rs_con.commit()
	
		rs_query = "SELECT \"column\" from pg_table_def where tablename='mortgage_rate'"
		rs_cur.execute(rs_query)
		rs_columns = [x[0] for x in rs_cur.fetchall()]
		rs_columns.remove('created_at')
	
		########################
		# Create insert query
		########################
		print("Inserting values...")
		rs_query = "INSERT INTO mortgage_rate(created_at,{}) VALUES".format(",".join(rs_columns))
		for idx in mortgage_df.index:
			rs_query = rs_query + "(GETDATE(),"
			for cur_col in rs_columns:
				if ((mortgage_df.loc[idx,cur_col] == ' ') | (mortgage_df.loc[idx,cur_col] == '')):
					rs_query = rs_query + "NULL,"
				elif (pd.isnull(mortgage_df.loc[idx,cur_col]) == True):
					rs_query = rs_query + "NULL,"
				else:
					rs_query = rs_query + "$${}$$,".format(mortgage_df.loc[idx,cur_col])
			rs_query = rs_query[:-1] + "),"
	
		rs_query = rs_query[:-1]
	
		rs_cur.execute(rs_query)
		rs_con.commit()
		print("Done")
		statusCode = 200
		message = 'OK'

		# Verify update
		max_date_query = "select MAX(mortgage_rate_date) mortgage_rate_date from mortgage_rate"
		rs_cur.execute(max_date_query)
		latest_mortage_rate_date = [x[0] for x in rs_cur.fetchall()][0]
		if latest_mortage_rate_date <= dt.datetime.today().date():
			report_to_rollbar("Mortgage Rate was not updated", "", "")
		
		

	except Exception as e:
		print('REDSHIFT {} failed . {}'.format(os.environ["ENV"], e))
		statusCode = 500
		message = str(e)
		stack = traceback.format_exc()
		report_to_rollbar(e, str(stack), str(sys.exc_info()))
		
	#############################
	# Close redshift connection
	#############################
	rs_cur.close()
	rs_con.close()
	response = {
		'statusCode': statusCode,
		'message': message
	}	
	return response


def report_to_rollbar(err, stack, exc_info):
	secure_log(":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")

	secure_log(err)
	secure_log(stack)
	secure_log(exc_info)

	sns = boto3.client('sns')
	
	py_err = { 'name': "MortageRateException", 'message': str(err), 'type': "error", 'trace': {
		'exc_info': "\"{}\"".format( str(exc_info.replace("\"", "'") ) ), 
		'stack': "\"{}\"".format(str(stack).replace("\"", "'"))
		} 
	}
	
	rollbar_error = { 'error': py_err, 'referer': 'MortgageRate'}
	rollbar_error = json.dumps(rollbar_error)

	response = sns.publish(
		TopicArn=os.environ["TOPIC_ROLLBAR"], 
		Subject="Production Error in Leads Lambda",   
		Message=rollbar_error
	)
	secure_log("Response: {}".format(response))
