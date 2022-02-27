#! /usr/bin/env python

#################################################################
#
# Created by DKJ ... 12/16/2016
#
# Gives the entry-level structure of each table within the
# the ProPair database ecosystem
#
# 1) Tables should be updated as necessary when extra columns
# are added over time
#
#################################################################

import pandas as pd 
import numpy as np 
import os
import time
import pdb
import sys
import requests
import base64, json
import urllib
import time
import datetime as dt
import re
from json import loads,dumps
from threading import Thread
from pplibs.logUtils import secure_log
from collections.abc import Iterable
from itertools import chain

##############################################
############## Class Velocify ################
##############################################
#
# Created by Devon K Johnson, 4/23/2017
#
# Class deals with read data from, write data
# to velocify through the SOAP API
#
##############################################
class InContact:
	
	def __init__(self,ACCOUNT,BASE64, incontact_user, incontact_pass):

		self.ACCOUNT = ACCOUNT
		self.user = incontact_user 
		self.passwd = incontact_pass 
		self.base64 = BASE64

	def getToken(self):
	
		url = "https://api.incontact.com/InContactAuthorizationServer/Token"

		header = {'Authorization': 'basic {}'.format(self.base64)}
		payload = {'grant_type' : "password", 'username' : self.user, 'password' : self.passwd, 'scope': ""}

		try:	
			r = requests.post(url,headers=header,data=payload)
		except Exception as e:
			secure_log('requests.post failure {}'.format(e))
			r = None 
	
		return r 

def pull_report(report_id, account_config, config_variables, user, password):
	VERSION = "17.0"

	from_now_min = int(config_variables['fromNowMinutes']) if 'fromNowMinutes' in config_variables else 15

	secure_log('::::: CALL LOG LOOKBACK (minutes) . {}'.format(config_variables['fromNowMinutes']))

	start_date = dt.datetime.now() - dt.timedelta(minutes=from_now_min)
	end_date = dt.datetime.now() + dt.timedelta(hours=1)

	##### USE THIS FOR BACKLOG ... RUN 24 HOURS AT A TIME
	#start_date = dt.datetime(2021, 2, 26, 0, 0, 0, 0)
	#end_date = dt.datetime(2021, 2, 27, 5, 0, 0, 0)

	start_date_str = start_date.strftime('%Y-%m-%d %H:%M:%S')
	end_date_str = end_date.strftime('%Y-%m-%d %H:%M:%S')

	inc = InContact(account_config['name'],base64.b64encode(b'DataExtractionLayer@ProPair:4598179').decode('ascii'), user, password)

	r = inc.getToken()
	access_json = json.loads(r.content)

	##############################
	# Check if token is valid
	##############################
	status_code = 401
	while (status_code == 401):
		BASEURL = str(access_json['resource_server_base_uri'])
		accessToken = str(access_json['access_token'])
	
		#Add all necessary headers
		header_param = {'Authorization': 'bearer ' + accessToken,'content-Type': 'application/x-www-form-urlencoded','Accept': 'application/json, text/javascript, */*'}
	
		############################
		# Create InContact Report
		############################
		fileName = 'propair_reportId{}_{}_{}.csv'.format(report_id,start_date_str.replace('-',''),end_date_str.replace('-',''))

		# Make http post request
		https_url = "{}/services/v{}/report-jobs/datadownload/{}?startDate={}&endDate={}&saveAsFile=false&includeHeaders=true&filename={}"\
							.format(BASEURL,VERSION,report_id,start_date_str,end_date_str, fileName)

		data_response = requests.post(https_url,headers=header_param)
	
		status_code = data_response.status_code
		secure_log('GET DATA: HTTPS Request Status Code . {}'.format(data_response.status_code))
		if (data_response.status_code == 401):
			secure_log('Token Expired . Create New Token')
			r = inc.getToken()
	
			access_json = json.loads(r.content)
	
	if (data_response.status_code == 200):
		encoded_report_data = json.loads(data_response.content)
		report_data = base64.b64decode(encoded_report_data['file']).decode('ascii')
	
		####################
		# Find # of columns
		####################
		#report_data_df = pd.DataFrame([x.split(',') for x in report_data[:-2].split('\r\n')])
		TMP_report_data = report_data[:-2].split('\r\n')
		Ncols = len(TMP_report_data[0].split(",")) #Should always work since first row in column name
		#report_data_df = pd.DataFrame([x.split(',') if len(x.split('"""')) == 1 else str("test,"*Ncols).split(',') for x in TMP_report_data])
		
		###########################################################################################
		# Split into columns
		# 1) if len(x.split) == 1, just split
		# 2) if len(x.split) > 1, we have text potentially with commas, must treat separately
		# 	2a) Replace strings that cause extra columns ,""" and """,
		#	2b) Split by """
		#	2c) Split , in even indices / don't split odd indices
		#	2d) Stitch together with chain
		###########################################################################################
		report_data_df = pd.DataFrame([x.split(',') if len(x.split('"""')) == 1 \
										else list(chain.from_iterable(item if isinstance(item,Iterable) \
													and not isinstance(item, str) else [item] \
													for item in [t.split(",") if (iii % 2) == 0 else t \
													for iii,t in enumerate(x.replace(',""",','"""').replace(',"""','"""').replace('""",','"""').split('"""'))])) \
											for x in TMP_report_data])

		report_data_df.columns = report_data_df.iloc[0]
		report_data_df = report_data_df.drop(0)
		try:
			report_data_df = report_data_df.drop(columns=[None])
			secure_log('Found None as Column names. Dropping..')
		except:
			pass
		report_data_df = report_data_df.reset_index(drop=True) 
		secure_log('(ReportId,Start Date,End Date) . ({},{},{}) . NUMBER OF RECORDS . {}'.format(report_id,start_date_str,end_date_str,np.shape(report_data_df)))
			
		output_df = report_data_df.copy(deep=True)

		if 'DateOfCall' in output_df.columns:
			secure_log("PRE-FILTER  . len(output_df) = {}".format(len(output_df)))
			output_df = output_df[output_df['DateOfCall'] != '']
			secure_log("POST-FILTER . len(output_df) = {}".format(len(output_df)))

			secure_log("Convert 'DateOfCall' to datetime")
			output_df['DateOfCall'] = pd.to_datetime(output_df['DateOfCall'],format = '%m/%d/%Y %I:%M:%S %p')
			secure_log("COMPLETE . Convert 'DateOfCall' to datetime")
		
		return output_df
	else:
		err = {"message": "Unable to retrieve report", "code": data_response.status_code, "content": data_response.content}
		raise Exception(err)
