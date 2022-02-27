import sys
import os
import traceback

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
import pytz
import xmltodict

from db_utils import insert_to_db
from db_utils import get_unique_agents
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log

sns = boto3.client('sns')

##### Redshift env variables 
db_host = os.environ["REDSHIFT_ENDPOINT"]
db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
#db_pass = os.environ["REDSHIFT_DB_PASSW"]
db_port = 5439


#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]), 
	WithDecryption=True
) 

db_pass = response['Parameter']['Value']

velocify_get_reports_url	= "https://service.leads360.com/ClientService.asmx/GetReports"
velocify_report_url	 = "https://service.leads360.com/ClientService.asmx?op=GetReportResults"


def getAgentMapping(cur, account, account_id):
	secure_log("::::: Fetching Agent Profile Info for account {}".format(account))
	agent_mapping = {}
	try:
		query = 'SELECT id as propair_id, agent_id, name_velocify from agent_profiles WHERE account_id={};'.format(account_id)
		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			agent_mapping[ line[2] ] = { 'propair_id': line[0], 'agent_id': line[1] }
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
		secure_log( "I am unable to connect to the database")

	return agent_mapping

def getCampaignMapping(cur, account, account_id):
	secure_log("::::: Fetching Campaign Info for account {}".format(account))
	campaign_mapping = []
	
	try:
		query = 'SELECT source, source_detail, campaign_group, source_channel, ialgo_source from source_detail_lookup WHERE account_id={};'.format(account_id)
		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			campaign_mapping.append( { 'source': line[0], 'source_detail': line[1], 'campaign_group': line[2], 'source_channel': line[3], 'ialgo_source': line[4] } )
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
		secure_log( "I am unable to connect to the database")

	return campaign_mapping

def update_agent_events( account_id, agent_id, lead_id, event, event_datetime, cur, conn):
	eastern_tz	  = pytz.timezone('US/Eastern') #TODO: This will vary from customer to customer
	timestamp	   = datetime.datetime.strptime(event_datetime, '%Y-%m-%dT%H:%M:%S')
	localized	   = eastern_tz.localize(timestamp, is_dst=False)
	utc_time		= localized.astimezone(pytz.utc)
	
	log_date_utc = utc_time.strftime("%Y-%m-%d %H:%M:%S.%f") #2018-08-25 00:00:58.215244

	try:
		query = "INSERT INTO agent_events(account_id, agent_id, lead_id, event, event_datetime, created_at, updated_at) VALUES({}, {}, {}, '{}', '{}', '{}', '{}')".format(account_id, agent_id, lead_id, event, log_date_utc, datetime.datetime.now(), datetime.datetime.now())
		cur.execute(query)
		conn.commit()
		secure_log( "Successfully updated agent_events ---------> ")
	except Exception as err:
		conn.rollback()
		if "duplicate key value violates unique constraint" not in str(err):
			secure_log( "Unable to update agent_events table")
			stack = traceback.format_exc()
			report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
			secure_log(err)
	  
def insert_agent_profiles( account_id, leads, cur, conn):
	query = 'SELECT agent_id from agent_profiles WHERE account_id={} AND agent_id IS NOT NULL;'.format(account_id)
	cur.execute(query)
	data = cur.fetchall()
	agent_ids = []
	new_agents = []

	query = 'SELECT coalesce(max(id), 0) from agent_profiles'
	cur.execute(query)
	max_id = cur.fetchone()[0]

	for line in data:
		agent_ids.append(line[0])

	secure_log(sorted(agent_ids))
	secure_log('Total Agents . {}'.format(len(sorted(agent_ids))))

	new_agents = get_unique_agents(leads, account_id, agent_ids)

	secure_log( "{} New Agents found".format(len(new_agents)))

	if len(new_agents) > 0:
		secure_log( "::::: Inserting New Agent Profiles")
		id_counter = int(max_id)
		try:
			query = "INSERT INTO agent_profiles(id, account_id, agent_id, name, name_velocify, first_name, last_name, role, created_at, updated_at) VALUES "
			for i, a in enumerate(new_agents):
				id_counter += 1
				query += "({}, {}, {}, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$, $${}$$)".format(id_counter, a['account_id'], \
													a['agent_id'], a['name'],a['name_velocify'],a['first_name'],a['last_name'],\
													'Loan Officer', a['created_at'], a['updated_at'])
				if i + 1 < len(new_agents):
					query += ', '
				else:
					query += ';'
			cur.execute(query)
			conn.commit()
			secure_log( "::::: Successfully Inserted Agent Profiles")
		except Exception as err:
			stack = traceback.format_exc()
			report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
			conn.rollback()
			secure_log( "I am unable to connect to the database")
	else:
		secure_log( "::::: No New Agent Profiles To Insert")

def sanitize_field(field):
	return re.sub("_+x00..", "", field)

def get_leads(account_meta, account_config, report_name, is_data_pull=False):

	def fetch_report(name):
		secure_log("::::: Fetching Report List for account {}".format(account_meta['account']))
		
		# defining a params dict for the parameters to be sent to the API
		payload = {'username': account_meta['velocify_username'], 'password': account_meta['velocify_password']}

		# sending get request and saving the response as response object
		r = requests.post(velocify_get_reports_url, data = payload)
		
		# extracting data in XML format
		secure_log("------------>")
		
		report = None
		data = None
		
		try:
			data = xmltodict.parse(r.content, attr_prefix='', dict_constructor=dict)
		except Exception as e:
			secure_log('xmltodict failure . {}'.format(e))

		if (isinstance(data, dict)):		
			for child in data['Reports']['Report']:
				if(name in child['ReportTitle']):
					report = child
		else:
			secure_log("::::: Error parsing report {}".format(name))

		return report

	def fetch_leads(account_meta, report, filters=[]):

		headers = {'Content-Type': 'text/xml; charset=utf-8',
			"Host": "service.leads360.com",
			"Content-Length": "length",
			"SOAPAction": "https://service.leads360.com/GetReportResults"}
		
		filters_xml = ""

		for item in filters:
			filters_xml += """<FilterItem FilterItemId="{0}">{1}</FilterItem>""".format(item['id'], item['value'])

		body = """<?xml version="1.0" encoding="UTF-8"?>
			<soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
				<soap:Body>
					<GetReportResults xmlns="https://service.leads360.com">
						<username>{0}</username>
						<password>{1}</password>
						<reportId>{2}</reportId>
						<templateValues>
							<FilterItems>
								{3}
							</FilterItems>   
						</templateValues>
					</GetReportResults>
				</soap:Body>
			</soap:Envelope>""".format(account_meta['velocify_username'], 
				account_meta['velocify_password'],
				report['ReportId'],
				filters_xml)

		try:
			r = requests.post(velocify_report_url  , data=body, headers=headers)
		except Exception as e:
			try:
				secure_log('RETRY 1 . requests.post failure . {}'.format(e))
				r = requests.post(velocify_get_report_url,data=payload)
			except:
				try:
					secure_log('RETRY 2 . requests.post failure . {}'.format(e))
					r = requests.post(velocify_get_report_url,data=payload)
				except:
					secure_log('requests.post failure . {}'.format(e))
					r = None

		try:
			data = xmltodict.parse(r.content, attr_prefix='', dict_constructor=dict)
		except Exception as e:
			try:
				secure_log('xmltodict failure . {}'.format(e))
				XMLdecode = r.content.decode('utf-8')
				#XMLcorrected = r.content.encode('utf-8') 
				#XMLcorrected = re.sub('&.+[0-9]+;', '', XMLdecode)
				XMLcorrected = XMLdecode.replace('&','')
				data = xmltodict.parse(XMLcorrected)
				secure_log('xmltodict success . {} . ERROR CORRECTED'.format(e))
			except Exception as e:
				secure_log('2nd xmltodict failure . {}'.format(e))
				data = {}

		return data

	if(is_data_pull is True):
		start_date = datetime.datetime.strptime(account_meta['start_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M %p")
		end_date = datetime.datetime.strptime(account_meta['end_date'],"%m/%d/%Y %H:%M:%S").strftime("%m/%d/%Y %I:%M %p")

		report = fetch_report(report_name)
		secure_log("::::: Found Report ID: {} . {}".format(report['ReportId'],report['ReportTitle']))
		
		if (report is not None):
			secure_log("::::: Found Report ID for ProP Data Update: {}".format(report['ReportId']))

			filters = []

			if ('FilterItems' in report):
				if('FilterItem' in report['FilterItems']):
					if (isinstance(report['FilterItems']['FilterItem'], list)):
						for child in report['FilterItems']['FilterItem']:
							if (child['Operator'] in ['GreaterThanOrEqualTo', 'GreaterThan']):
								filters.append({'id': child['FilterItemId'], 'value': start_date})
							elif(child['Operator'] in ['LessThanOrEqualTo', 'LessThan']):
								filters.append({'id': child['FilterItemId'], 'value': end_date})
					else:
						secure_log(":::: Error! Missing filters", report)
						raise Exception("Missing filters")
					
			if (len(filters) < 2):
				secure_log(":::: Error! Missing filter ids", report)
				raise Exception("Missing filter ids")


			secure_log("::::: Fetching Report")
			leads_data = fetch_leads(account_meta, report, filters)

			return leads_data['soap:Envelope']['soap:Body']['GetReportResultsResponse']['GetReportResultsResult']
			
	else:
		secure_log("::::: No dates provided. Triggering incremental update.")
		
		min_from =  account_config['leads_incremental_min_from'] if  'leads_incremental_min_from' in account_config else 30
		secure_log("::::: Incremental update set to: {} min".format(min_from))
		
		start_date = datetime.datetime.utcnow() - datetime.timedelta(hours=8, minutes=int(min_from))
		start_date = start_date.strftime("%m/%d/%Y %I:%M %p")
		report = fetch_report(report_name)
		
		if(report is not None):
			secure_log("::::: Found Report ID for ProP Data Update: {}".format(report['ReportId']))

			secure_log("::::: Fetching Report")
			leads_data = fetch_leads(account_meta, report)

			return leads_data['soap:Envelope']['soap:Body']['GetReportResultsResponse']['GetReportResultsResult']


def get_global_attr(cur, account, table_name, include_variable = False):
	secure_log("::::: Fetching Global Attribute Mapping {} for account {}".format(table_name, account))
	attribute_map = []
	include_variable_string = "AND include_variable = 1" if include_variable == True else ""

	try:		
		query = "SELECT propair_field, customer_field_name, customer_original_field_name, customer_split, datatype FROM global_attribute_lookup L INNER JOIN accounts A on L.account_id = A.id WHERE A.name = '{0}' AND (table_name='{1}' OR table_name ='all') {2} AND ( customer_field_name IS NOT NULL OR customer_original_field_name IS NOT NULL OR  customer_split IS NOT NULL );".format(account.lower(), table_name, include_variable_string)
		cur.execute(query)
		data = cur.fetchall()
		for line in data:
			attribute_map.append( { 'propair': line[0], 'customer': line[1], 'original': line[2], 'split': line[3], 'datatype': line[4] } )

		return attribute_map
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')

def update(event, context):
	try:
		if 'refresh' in event:
			secure_log("::::: Refreshing Cache.")
			get_account_config(None, True)
			secure_log("::::: Successfully Refreshed Cache")
			return


		account_meta = event
		account_id		  = account_meta['account_id']
		account_config	  = get_account_config(account_id)

		external_leads_table = account_meta['external_leads_table'] if 'external_leads_table' in account_meta else 'external_leads'
		external_lead_details_table = account_meta['external_lead_details_table'] if 'external_lead_details_table' in account_meta else 'external_lead_details'
		
		secure_log("::::: Connecting to database..")
		conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
		cur = conn.cursor()

		#get velocify info
		sq = 'SELECT name,velocify_username, velocify_password from accounts where id = {}'.format(account_id)
		cur.execute(sq)
		account_data = cur.fetchone()
		account_meta['account'] = account_data[0]
		account_meta['velocify_username'] = account_data[1]
		account_meta['velocify_password'] = account_data[2]

		account			 = account_meta['account']

		secure_log("::::: ACCOUNT CONFIG {}".format(account_config))
		secure_log("::::: ACCOUNT META", account_meta)

		attribute_map = get_global_attr(cur, account, 'lead')
		detail_attribute_map = get_global_attr(cur, account, 'lead_detail', include_variable=True)

		is_data_pull = False
	
		if (all(x in account_meta for x in ['start_date', 'end_date'])):
			is_data_pull = True
		
		report_name = 'ProP Data Lead Initial' if is_data_pull else 'ProP Data Update'
		leads_data = get_leads(account_meta, account_config, report_name, is_data_pull)

		if(leads_data != None):
			if ('Result' not in leads_data['ReportResults']):
				secure_log("::::: No leads found! Exiting..")
				raise Exception("Report Name: {} returned 0 leads".format(report_name))

			leads = []
			
			for result in leads_data['ReportResults']['Result']:
				lead = {}
				for lead_child in result:
					lead[ lead_child ] = result[lead_child]
				leads.append( lead )

			normalized_leads = []
			normalized_lead_details = []

			secure_log( "*************")
			secure_log( "Leads Retrieved {}".format(len(leads)))
			secure_log( "*************")

			# Make sure we're up-to-date in the agent_profiles table
			try:
				insert_agent_profiles(account_id, leads, cur, conn)
			except Exception as err:
				stack = traceback.format_exc()
				report_to_rollbar(err, str(stack), str(sys.exc_info()), '')


			agent_mapping = getAgentMapping(cur, account, account_id)
			campaign_mapping = getCampaignMapping(cur, account, account_id)

			secure_log( "Campaign Mapping ------->")
			#print(campaign_mapping)

			secure_log( "::::: Normalizing Leads")
			Nno_firstassign = 0
			Nno_loan_officer = 0
			Nno_user = 0
			Nno_profile_id_user = 0
			missing_users = []
			missing_first_assignment_users = []

			for lead in leads:
				normalized_lead = { 'account_id': account_id }
				normalized_lead_detail = { 'account_id': account_id }
				accepted_fields = account_config['accepted_split_fields'] if 'accepted_split_fields' in account_config else []

				#Sanitizing Field Names for Mapping
				new_lead = lead.copy()
				Ncol = len(new_lead.keys())
				for key, value in lead.items():
					if (sanitize_field(key) != key):
						new_lead[sanitize_field(key)] = lead[key]
						del new_lead[key]

				lead = new_lead.copy()	

				for attribute in attribute_map:
					#TODO: Handle ':' string splits
					if attribute['original'] in lead:
						normalized_lead[ attribute['propair'] ] = sanitize_and_cast( lead[ attribute['original'] ], attribute['datatype'], attribute['propair'], accepted_fields )
					elif ':' in attribute['customer']: 
						components = attribute['customer'].split(':')
						if components[0] in lead:
							values = lead[ components[0] ].split(attribute['split'])
							if(len(values) > int(components[1])):
								normalized_lead[ attribute['propair'] ] = sanitize_and_cast( values[ int(components[1]) ], attribute['datatype'], attribute['propair'], accepted_fields )
					elif attribute['customer'] in lead: 
						normalized_lead[ attribute['propair'] ] = sanitize_and_cast( lead[ attribute['customer'] ], attribute['datatype'], attribute['propair'], accepted_fields )
				
				#### Cleanup Zip Codes
				if 'property_zip_code_stated' in normalized_lead and normalized_lead['property_zip_code_stated'] is not None and len(normalized_lead['property_zip_code_stated']) > 5:
					normalized_lead['property_zip_code_stated'] = normalized_lead['property_zip_code_stated'][:5]

				#################################
				# Create lookup to profile table
				#################################
				
				profile_id_loan_officer_default	 = None
				profile_id_first_assignment_user	= None
				profile_id_user					 = None

				if('FirstAssignment_DistributionUser' in lead and lead['FirstAssignment_DistributionUser'] in agent_mapping and 'agent_id' in agent_mapping [ lead['FirstAssignment_DistributionUser'] ]) and agent_mapping [ lead['FirstAssignment_DistributionUser'] ]['agent_id'] != None: 
					profile_id_loan_officer_default  = agent_mapping [ lead['FirstAssignment_DistributionUser'] ]['propair_id']
					profile_id_first_assignment_user = agent_mapping [ lead['FirstAssignment_DistributionUser'] ]['propair_id']
					#update_agent_events( account_id, agent_mapping [ lead['FirstAssignment_DistributionUser'] ]['agent_id'], normalized_lead['account_lead_id'], "FIRST_ASSIGNMENT", lead['FirstAssignment_x002F_DistributionDate'], cur, conn )
				elif('FirstAssignment_DistributionUser' in lead):
					Nno_loan_officer = Nno_loan_officer+1
					if lead['FirstAssignment_DistributionUser'] not in missing_first_assignment_users:
						missing_first_assignment_users.append(lead['FirstAssignment_DistributionUser'])

				else:
					Nno_firstassign = Nno_firstassign+1 

				if('User' in lead and lead['User'] in agent_mapping and 'agent_id' in agent_mapping [ lead['User'] ]) and agent_mapping [ lead['User'] ]['agent_id'] != None: 
					profile_id_user = agent_mapping [ lead['User'] ]['propair_id']

					#if profile_id_first_assignment_user == None or profile_id_first_assignment_user != profile_id_user:
						# Only track assignment events in case the lead has been assigned to an agent different than the FA
					#   update_agent_events( account_id, agent_mapping [ lead['User'] ]['agent_id'], normalized_lead['account_lead_id'], "REASSIGNMENT", lead['LastAssignment_x002F_DistributionDate'], cur, conn )

				elif('User' in lead):
					if lead['User'] not in missing_users:
						missing_users.append(lead['User'])
					Nno_profile_id_user = Nno_profile_id_user+1 
				else:		 
					Nno_user = Nno_user+1 


				normalized_lead_detail['profile_id_loan_officer_default']   = profile_id_loan_officer_default
				normalized_lead_detail['profile_id_first_assignment_user']  = profile_id_first_assignment_user
				normalized_lead_detail['profile_id_user']				   = profile_id_user

				normalized_leads.append( normalized_lead )

				for attribute in detail_attribute_map:
					#TODO: Handle ':' string splits
					if attribute['original'] in lead:
						normalized_lead_detail[ attribute['propair'] ] = sanitize_and_cast( lead[ attribute['original'] ], attribute['datatype'], attribute['propair'], accepted_fields )
					elif ':' in attribute['customer']: 
						components = attribute['customer'].split(':')
						if components[0] in lead:
							values = lead[ components[0] ].split(attribute['split'])
							
							if(len(values) > int(components[1])):
								normalized_lead_detail[ attribute['propair'] ] = sanitize_and_cast( values[ int(components[1]) ], attribute['datatype'], attribute['propair'], accepted_fields )
					elif attribute['customer'] in lead: 
						normalized_lead_detail[ attribute['propair'] ] = sanitize_and_cast( lead[ attribute['customer'] ], attribute['datatype'], attribute['propair'], accepted_fields )

				
				
				normalized_lead_detail['profile_id_qualification']		  = None
				normalized_lead_detail['profile_id_processor']			  = None
				normalized_lead_detail['profile_id_sales_manager']		  = None
				normalized_lead_detail['profile_id_underwriter']			= None
				normalized_lead_detail['profile_id_encompass']			  = None
				
				######################################
				# Add Originated Loans
				######################################

				normalized_lead_detail['second_loan'] = 0
				
				########################################################
				# Grab lead domain 
				########################################################

				try:
					if 'email_stated' in normalized_lead and normalized_lead['email_stated'] != None:
						email_parts = normalized_lead['email_stated'].split('@')
						if len(email_parts) > 1:
							domain_parts = email_parts[1].split(".")
							if len(domain_parts) > 0:
								normalized_lead_detail['domain'] = domain_parts[0].upper()
								normalized_lead_detail['domain_tld'] = domain_parts[-1]
						else:
							normalized_lead_detail['domain'] = email_parts[0].upper()
							normalized_lead_detail['domain_tld'] = email_parts[0].upper()
				except:
					secure_log( "Error reading email domain")	
					secure_log(normalized_lead['email_stated']   )

				domain_list = account_config["domains"]["list"]
			
				### Put all others in OTHER 
				
				if 'domain' in normalized_lead_detail: normalized_lead_detail['BIN_domain'] = normalized_lead_detail['domain']
				if 'BIN_domain' in normalized_lead_detail and normalized_lead_detail['BIN_domain'] not in domain_list: normalized_lead_detail['BIN_domain'] = 'OTHER'
				
				###############################
				###### Group tld domains ######
				###############################
				if 'domain_tld' in normalized_lead_detail: normalized_lead_detail['BIN_domain_tld'] = normalized_lead_detail['domain_tld']
				

				########################################
				# Update lead_date (lead_details.ialgo)
				########################################		  
				lead_date = datetime.datetime.strptime( normalized_lead_detail['lead_datetime'], '%Y-%m-%dT%H:%M:%S' ).date()
				normalized_lead_detail['lead_date'] = str(lead_date)


				#######################################
				# TODO: Clean this up later, these attributes have been removed from lead_details
				#	   and are now part of the encompass (los) table. Is it safe to remove them 
				#	   from the global_attribute_mapping table?
				#######################################
				encompass_attributes = ['coborrower_race',
										'borrower_marital_status',
										'borrower_email',
										'enc_loan_purpose',
										'loan_status',
										'days_in_process',
										'coborrower_home_phone',
										'coborrower_email',
										'milestone_date_submittal',
										'milestone_date_approval',
										'status_detail',
										'borrower_home_phone',
										'borrower_last_name',
										'enc_velocify_lead_id',
										'account_loan_id',
										'borrower_mailing_address',
										'enc_adverse_date',
										'coborrower_mailing_zip',
										'enc_last_action_date',
										'borrower_cell_phone',
										'coborrower_work_phone',
										'down_payment_per',
										'borrower_income',
										'closing_date',
										'coborrower_last_name',
										'veteran_servicestatus',
										'borrower_work_phone',
										'coborrower_mailing_address',
										'enc_loan_folder',
										'coborrower_income',
										'coborrower_mailing_city',
										'enc_loan_amount',
										'coborrower_fico',
										'coborrower_cell_phone',
										'enc_app_date',
										'borrower_ethnicity',
										'enc_loan_officer',
										'coborrower_mailing_state',
										'purpose_description',
										'coborrower_first_name',
										'borrower_gender',
										'coborrower_gender',
										'milestone_date_docsigning',
										'milestone_date_funded',
										'loan_type',
										'loan_source',
										'borrower_first_name',
										'borrower_fico',
										'coborrower_ethnicity',
										'enc_file_started_date',
										'milestone_date_completion',
										'milestone_date_lock',
										'borrower_mailing_zip',
										'enc_nmls_id',
										'iapplication',
										'iclose',
										'ilock',
										'icontact',
										'iqualification',
										'ialgo',
										'home_value_corr',
										'loan_amount_corr',
										'ltv_calc',
										'milestone_date_contact',
										'milestone_date_qualification']

				for attr in encompass_attributes:
					if attr in normalized_lead_detail: 
						normalized_lead_detail.pop(attr, None) 

				############################################
				# iContact and iQualifictation
				# NOTE: This was moved to the view_external_lead_details Postgres view
				############################################
				
				normalized_lead_details.append( normalized_lead_detail )

			secure_log("::::: Done Processing Leads")
			secure_log("::::: Leads With No First Assignment: {}".format(Nno_firstassign))
			secure_log("::::: Leads With Profile ID Officer Default or Profile ID First Assignment User Not Found in Agent Profiles: {}".format(Nno_loan_officer))
			secure_log("::::: Leads With No User: {}".format(Nno_user))
			secure_log("::::: Leads With User Not Found in Agent Profiles: {}".format(Nno_profile_id_user))

			secure_log("::::: Missing Users: {}".format(len(missing_users)))
			secure_log("::::: Missing First Assignment Users: {}".format(len(missing_first_assignment_users)))

			if(is_data_pull is True):
				normalized_leads = sorted(normalized_leads, key=lambda k: k['account_lead_id']) 
				normalized_lead_details = sorted(normalized_lead_details, key=lambda k: k['account_lead_id'])

			#chunk_limit = int(account_config['leads_chunk_limit']) if 'leads_chunk_limit' in account_config else 10000

			secure_log("::::: Updating Leads Table")
			#stored_procedure = "sp_el_increment"
			unique_columns = ['account_id', 'account_lead_id']
			
			insert_to_db(normalized_leads, 'external_leads', cur, conn, unique_columns)
			secure_log( "::::: {} Leads Processed".format(len(normalized_leads)) )

			secure_log("::::: Updating Lead Details Table for account {}".format(account))
			#stored_procedure = "sp_eld_increment"

			insert_to_db(normalized_lead_details, 'external_lead_details', cur, conn, unique_columns)
			secure_log( "::::: {} LeadDetails Processed".format(len(normalized_lead_details)) )
	   
		else:
			secure_log("::::: Report not found.")

		cur.close()
		conn.close()
		secure_log("::::: All Done.")
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
		conn.rollback()


def sanitize_and_cast(val, to_data_type, field, accepted_fields):
	if(val == None):
		return None
	elif( "DATE" in to_data_type ):
		### Check if string is a malformed date ###
		#print( "DATE DEBUGGER")

		match = re.match(r"(^20\d{2}$)", val) #2005, 2015, 2021, etc
		if match and match.groups() and len(match.groups()) == 1:
			secure_log( "Formatted to: {}-01-01".format(val) )
			return "{}-01-01".format(val)

		match = re.match(r"(1/0/1900)", val) #1/0/1900
		if match and match.groups() and len(match.groups()) == 1:
			secure_log( "Formatted to: 1900-01-01")
			return "1900-01-01"

		match = re.match(r"(0\d{1}|1[0-2])([0-2]\d{1}|3[0-1])(20\d{2})", val) #06012012
		if match and match.groups() and len(match.groups()) == 3:
			secure_log( "Formatted to: {}-{}-{}".format(match.groups()[2], match.groups()[0], match.groups()[1]) )
			return "{}-{}-{}".format(match.groups()[2], match.groups()[0], match.groups()[1])

		return val
	elif( "CHAR" in to_data_type ):
		#TODO: Sanitize before casting
		try:
			if(val.upper() == "NAN" or val.upper() == "NA" or val.upper() == "None"):
				return None
			elif isinstance(val, bytes):
				return val.decode('unicode_escape')
			else:	
				return val.encode('ascii','ignore').decode('unicode_escape')

		except Exception as e:
			secure_log("::::: String sanitization error! Setting to blank. Field: {}, Value: {}".format(field, val))
			secure_log(e)
			return ''
			
	elif( "INT" in to_data_type or "FLOAT" in to_data_type or "DOUBLE" in to_data_type):
		#TODO: Sanitize before casting
		if(isinstance(val, str)):
			if(val == 'True' or val == 'YES'):
				return 1
			elif(val == 'False' or val == 'NO'):
				return 0
			else:
				numvalue = None
				orig_val = val

				#print('BEFORE: field,val = {},{}'.format(field,val))
				# TODO: Handle 35%Down,35%Down etc use case
				# if ',' in val and 'Down' in val: 
				#	 numvalue = val.split(',')[0] 
				# else: numvalue = val
				#if 'Zero' in val:
				#	 numvalue = "0"
				#elif '5%' in val:
				#	 numvalue = "5"
				#elif 'More Than 50%' in val:
				#	 numvalue = "50"
				if 'Down' not in orig_val:
					numvalue = re.sub("[^eE0-9.+-]", "", str(val))
				else:
					secure_log('ERROR: (DOWN in val): field,orig_val,val = {},{},{}'.format(field,orig_val,val))
					numvalue = None 

				#print('BEFORE SPLIT: field,val,numvalue = {},{},{}'.format(field,val,numvalue))
				if (numvalue is not None):
					try:
						if field in accepted_fields:
							x = float(re.sub("(?<!^)[+-]", "", str(numvalue)))
							numvalue = ((int(str(int(x))[0:len(str(int(x)))//2]) + int(str(int(x))[len(str(int(x)))//2:len(str(int(x)))]))//2) if int(x) > 100000000 else x
						else:
							numvalue = re.sub("(?<!^)[+-]", "", str(numvalue))
					except Exception as e:
						secure_log('ERROR (numvalue): field,orig_val,val,numvalue = {},{},{},{}'.format(field,orig_val,val,numvalue))
						secure_log(e)

				#print('AFTER SPLIT:  field,val,numvalue = {},{},{}'.format(field,val,numvalue))
				
				if 'Down' in orig_val:
					secure_log('AFTER: field,orig_val,val,numvalue = {},{},{},{}'.format(field,orig_val,val,numvalue))

				if(numvalue is not None):
					try:
						final_val = int( float( numvalue ) )

						#If phone numer is greater than 10 digits 
						#Set to -2. Identifying it as a bad phone number
						if "phone" in field and final_val > 100000000000:
							final_val = -2

						return final_val
					except Exception as e:
						secure_log('ERROR (numvalue is not None): {},{}'.format(val,e))
						return -1
				else:
					secure_log('ERROR: numvalue is None')
					secure_log( "Could not parse {} to {}.".format(val, to_data_type) )
					return -1
		else:
			return val
	else:
		return val

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


def insert_or_update_database( records, table, cur, conn, account, stored_procedure, chunk_num=None):
	## fetch char fields to implement size trunc
	chunk = " Chunk: {}".format(chunk_num) if chunk_num != None else ""
	secure_log(":::::{} - Upserting {} records to DB".format(chunk, len(records)))	

	char_column_dict = get_char_fields(cur, table)

	#print( "****executing upsert****")
	query =  "SELECT \"column\",type from pg_table_def where tablename='{}'".format(table)
	cur.execute(query)
	data = cur.fetchall()
	values	  = "VALUES "
	columns	 = {}
	ignored_columns = ['id', 'created_at', 'updated_at']

	for line in data:
		if line[0] not in ignored_columns:
			columns[line[0]] = {"name": line[0], "type": line[1]}

	query		= 'INSERT INTO {0}({1})'.format(table, ', '.join(columns.keys()))

	for i, record in enumerate(records):

		record_values = []
		date = datetime.datetime.now()

		for x, key in enumerate(columns):
			value = record[key] if key in record else None
			data_type = columns[key]['type']

			if ('integer' in data_type and value != None):
				max = 2147483647
				value = max if int(value) > max else int(value) 

			if(key in ['updated_at', 'created_at']):
				value = '$${}$$'.format(date)
			elif value == None:
				value = "NULL" 
			else:
				value = "$${}$$::{}".format(value, data_type)

			record_values.append(value)

		
		values += "({})".format(', '.join(record_values))
		if(i < len(records)-1):
			values += ", "
		
	query  += values
	sp_increment = "call {}()".format(stored_procedure)

	# query += " ON CONFLICT (account_id, account_lead_id) DO UPDATE SET "
	
	# date = datetime.datetime.now()

	# for x, key in enumerate(columns):
	#	 query += '{} = EXCLUDED.{}'.format(key, key)
	#	 if(x < len(columns)-1):
	#		 query += ", "
	#	 else:
	#		 query += ", updated_at = $${}$$;".format(date)
	
	#print("::::: Insert Query: {}".format(query) )

	try:
		cur.execute(query)
		secure_log("::::: Rows Affected in Stage Table: {}".format(cur.rowcount) )
		cur.execute(sp_increment)
		secure_log("::::: Rows Affected in Real Table: {}".format(cur.rowcount) )		
		conn.commit()
	except Exception as err:
		stack = traceback.format_exc()
		report_to_rollbar(err, str(stack), str(sys.exc_info()), '')
		secure_log("----> DB error: " + str(err))
		conn.rollback()
		
		
def report_to_rollbar(err, stack, exc_info, account):
	secure_log( ":::::::::: GOT AN ERROR TO REPORT :::::::::::::::")

	secure_log(str(err))
	secure_log(stack)
	secure_log(exc_info)

	sns = boto3.client('sns')
	
	py_err = { 'name': str(err), 'message': "LeadsDataArchException", 'type': "error", 'trace': {
		'exc_info': "\"{}\"".format( str(exc_info.replace("\"", "'") ) ), 
		'stack': "\"{}\"".format(str(stack).replace("\"", "'"))
		} 
	}
	
	rollbar_error = { 'error': py_err, 'referer': 'Leads', 'account': account }
	rollbar_error = json.dumps(rollbar_error)

	response = sns.publish(
		TopicArn=os.environ["TOPIC_ROLLBAR"], 
		Subject="Production Error in Leads Lambda",   
		Message=rollbar_error
	)
	secure_log("Response: {}".format(response))
			
