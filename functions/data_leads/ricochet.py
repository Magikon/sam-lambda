import os
import boto3
import json
import re
import pandas as pd
import numpy as np
import sys
import datetime as dt
import dateutil

from utils import meld_columns, get_url
from pplibs.logUtils import get_account_config, secure_log

class Ricochet(object):

	def __init__(self, con, account):

		self.con = con
		self.cur = con.cursor()
		self.account = account
		self.token = self.get_token()
		self.base_url = "https://r2.ricochet.me/api/v4/leads"
		self.header = {'x-auth-token': self.token}
		self.rollbar_topic = os.environ["TOPIC_ROLLBAR"]

		self.config = get_account_config(self.account['id'], cache=False)

		try:
			self.config_variables = self.config['leads_producer']['variables']
		except KeyError:
			raise Exception("Unable to find producer configuration in DynamoDB: <leads_producer.variables>")

	def get_token(self): 
		client = boto3.client('ssm')
		response = client.get_parameter(
			Name="/{}/{}/ricochet-token".format(os.environ["ENV"], self.account['name']), 
			WithDecryption=True
		) 
		return response['Parameter']['Value']

	def get_max_lead(self):
		sql = """
			SELECT max(account_lead_id) as id FROM external_leads WHERE account_id = {}
		""".format(self.account['id'])
		self.cur.execute(sql)
		return meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])[0]

	def get_max_updated_at(self):
		#SELECT max(account_lead_id) as id FROM external_leads WHERE account_id = {}
		sql = """
			SELECT max(updated_at) as updated_at FROM external_leads WHERE account_id = {}
		""".format(self.account['id'])
		self.cur.execute(sql)
		tmp = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])[0]

		# Subtract 10 hours to be UTC - 1 so all times zones are matched
		tmp['updated_at'] = tmp['updated_at'] - dt.timedelta(hours = 10)

		return tmp

	def get_total_pages(self, limit = 1000):
		#############################################
		# Grab total pages
		#############################################

		url =  self.base_url + '?scroll=1&sort=updated_at&sort_order=desc'.format(limit)
		r, error = get_url( url , self.header)

		if (r.status_code == 200):
			output = json.loads(r.content)

			secure_log('Total Leads . {}'.format(output['total_results']))
			secure_log('Total Pages . {}'.format(output['total_pages']))
			secure_log('Per Page . {}'.format(output['per_page']))
			secure_log('Current Page . {}'.format(output['current_page']))
			
			return int(output['data']['leads']['total_pages'])
		else:
			raise Exception("Unable to retrieve total pages - Reason: {}, Error: {}".format(r.reason, error))

	def get_lead_action_list(self,sort_var,sort_order='desc',start_page=1,end_page=None,min_from_now=0):

		###############################################################
		# Current Date
		# Current Date - 9 hrs (UTC -> PST correction) - min_from_now
		###############################################################
		if (sort_var == 'updated_at'):
			date_threshold = dt.datetime.now() - dt.timedelta(hours = 9) - dt.timedelta(minutes = min_from_now)
			secure_log('Date Threshold . {}'.format(date_threshold))

		url = '{}?scroll=1&sort={}&sort_order={}'.format(self.base_url,sort_var,sort_order)
		#r = requests.get(base_url,headers=header)
		r, error = get_url(url, self.header)
		output = json.loads(r.content)
		next_page = output['data']['leads']['next_page']

		col = output['data']['leads']['0'].keys()
		per_page = output['data']['leads']['per_page']
		all_int=range(0,per_page)
		total_pages = output['data']['leads']['total_pages']
		total_leads = output['data']['leads']['total_results']

		if (end_page is None):
			end_page = total_pages

		# TODO REMOVE AFTER TESTING
		#total_pages = total_pages - 10

		lead_data = output['data']['leads']
		secure_log('Total Leads . {}'.format(lead_data['total_results']))
		secure_log('Total Pages . {}'.format(lead_data['total_pages']))
		secure_log('Per Page . {}'.format(lead_data['per_page']))
		secure_log('Current Page . {}'.format(lead_data['current_page']))

		secure_log('Total Pages = {} . Records Per Page = {}'.format(total_pages,per_page))

		##############################
		# Populate first set of leads
		##############################
		all_leads = []
		all_actions = []
		if (start_page == 1):
			found_lead=True
			cur_child = 0
			while(found_lead):
				if (str(cur_child) in output['data']['leads'].keys()):
					all_leads.append(output['data']['leads'][str(cur_child)])
					all_actions.append(output['data']['leads'][str(cur_child)]['actions'])

					cur_child = cur_child + 1
				else:
					found_lead = False

		#############################################
		# Grab all data and put in data frame format
		#############################################
		found_final_lead = False
		for cur_page in range(1,end_page):
			secure_log('Page {:4d} of {:4d}'.format(cur_page,total_pages))
			url = output['data']['leads']['next_page']

			r, error = get_url(url, self.header)
			#r = requests.get(url,headers=header)
			output = json.loads(r.content)

			lead_data = output['data']['leads']

			if (cur_page >= start_page):
				found_lead=True
				cur_child = 0
				while(found_lead):
					if (str(cur_child) in output['data']['leads'].keys()):
						all_leads.append(output['data']['leads'][str(cur_child)])
						all_actions.append(output['data']['leads'][str(cur_child)]['actions'])

						#print('{} . {} . {} . {}'.format(str(cur_child),date_threshold,\
						#							pd.to_datetime(output['data']['leads'][str(cur_child)]['updated_at']),\
						#							output['data']['leads'][str(cur_child)]['id']))

						#################################
						# If 'updated_at' ...
						# stop when date_threshold is met
						#################################
						if (sort_var == 'updated_at'):
							if (date_threshold >= pd.to_datetime(output['data']['leads'][str(cur_child)]['updated_at'])):
								secure_log('FINAL . {} . {} . {} . {}'.format(str(cur_child),date_threshold,\
														pd.to_datetime(output['data']['leads'][str(cur_child)]['updated_at']),\
														output['data']['leads'][str(cur_child)]['id']))
								found_final_lead = True
								break

						cur_child = cur_child + 1
					else:
						found_lead = False

			if found_final_lead:
				break

			#print([x['id'] for x in all_leads])

		return all_leads,all_actions

#	def get_update_lead_list(self, max_updated_at, min_from_now):
#
#		###############################################################
#		# Current Date
#		# Current Date - 9 hrs (UTC -> PST correction) - min_from_now
#		###############################################################
#		date_threshold = dt.datetime.now() - dt.timedelta(hours = 9) - dt.timedelta(minutes = min_from_now)
#
#		#############################################
#		# Grab list of leads per page
#		#############################################
#		
#		limit = 1000
#		no_max_lead = True
#
#		secure_log('Get Lead List')
#		scroll=1
#		next_page = ''
#		all_lead = []
#		while(no_max_lead):
#
#			secure_log('Page {:4d}'.format(scroll))
#			if (next_page == ''):
#				url = '{}?scroll=1&sort=updated_at&sort_order=desc'.format(self.base_url)
#			else:
#				url = next_page
#
#			r, error = get_url(url, self.header)
#			if (r.status_code == 200):
#
#				output = json.loads(r.content)
#				next_page = output['data']['leads']['next_page']
#
#				#print(output)
#				max_key = max([int(x) for x in output['data']['leads'].keys() if x.isdigit() == True])
#				found_lead=True
#				cur_child = 0
#				for iii in range(0,max_key+1):
#					if (str(iii) in output['data']['leads'].keys()):
#						secure_log('{} . {} . {} . {}'.format(str(iii),date_threshold,\
#													pd.to_datetime(output['data']['leads'][str(iii)]['updated_at']),\
#													output['data']['leads'][str(iii)]['id']))
#						all_lead.append(output['data']['leads'][str(iii)])
#
#						#if (max_updated_at['updated_at'] >= pd.to_datetime(output['data']['leads'][str(iii)]['updated_at'])):
#						if (date_threshold >= pd.to_datetime(output['data']['leads'][str(iii)]['updated_at'])):
#							secure_log('FINAL . {} . {} . {} . {}'.format(str(iii),date_threshold,\
#														pd.to_datetime(output['data']['leads'][str(iii)]['updated_at']),\
#														output['data']['leads'][str(iii)]['id']))
#							no_max_lead = False
#							break
#
#			else:
#				raise Exception("Unable to retrieve lead list - Reason: {}, Error: {}".format(r.reason, error))
#	
#			scroll = scroll + 1	
#
#		return all_lead

	def get_lead_data(self,all_leads):

		Nlead = 0
		all_data = []
		total_leads = len(all_leads)
		for cur_lead in all_leads:

			cur_id = str(cur_lead['id'])
			Nlead = Nlead + 1
			errcnt_call=0
			errcnt=0
			if (Nlead % 200 == 0):
				secure_log('Nlead . {:7d} of {:7d}'.format(Nlead,total_leads))

			#base_url = "https://r2.ricochet.me/api/v4/leads/{}".format(cur_id)
			#r = requests.get(base_url,headers=self.header)

			url = "{}/{}".format(self.base_url, cur_id)
			r, error = get_url(url, self.header)

			if (r.status_code == 200):
				output = json.loads(r.content)

				lead_data = output['data']['lead']

				for cur_del in self.config_variables['field_filters']:
					if cur_del in output['data']['lead'].keys():
						del lead_data[cur_del]

				FCD_url = "{}/{}/{}".format(self.base_url, cur_id,'call-history')
				rr, error = get_url(FCD_url, self.header)
				if (rr.status_code == 200):
					casted_dates=[dateutil.parser.parse(x['created_at']) for x in json.loads(rr.content)['data']['calls']]
					casted_dates.sort()
					if (len(casted_dates)>0):
						output['data']['lead']['First Call Date'] = str(casted_dates[0])
				else:
					errcnt_call+=1
					secure_log("Unable to retrieve lead from call-history; lead ID: {} - Reason: {}, Error: {}, Cumulative Error Count: {}".format(cur_id, rr.reason, error,errcnt_call))
			
				all_data.append(output['data']['lead'])
			else:
				errcnt+=1
				secure_log("Unable to retrieve lead; lead ID: {} - Reason: {}, Error: {}, Cumulative Error Count: {}".format(cur_id, rr.reason, error,errcnt))

		if(errcnt >0 or errcnt_call>0):
			py_err = ProPairError(f"{errcnt} leads failed to retrieve and {errcnt_call} failed to retrieve call history" , "Leads_Producer", exec_info=sys.exc_info(), stack=traceback.format_exc())
			py_err.account = self.account
			report_to_rollbar(self.rollbar_topic, py_err)

		return all_data

	def get_lead_updates(self):

		max_lead = self.get_max_lead()
		max_updated_at = self.get_max_updated_at()
		min_from_now = int(self.config_variables['min_from_now']) if 'min_from_now' in self.config_variables else 60
		secure_log('Minute Lookback . {}'.format(min_from_now))

		#lead_list = self.get_lead_list(max_updated_at,min_from_now)
		all_leads,all_actions = self.get_lead_action_list('updated_at','desc',min_from_now=min_from_now)

		##################################################
		# Extract all actions (events) from ricochet logs
		##################################################
		profile_dict = self.get_event_logs(all_actions)

		#############################################
		# Grab data from each individual lead 
		#############################################

		Nlead = 0
		all_data = []

		secure_log('Total Leads, Unique Leads . {},{}'.format(len(sorted([x['id'] for x in all_leads])),len(set(sorted([x['id'] for x in all_leads])))))
		#print(sorted([x['id'] for x in all_leads]))

		all_data = self.get_lead_data(all_leads)

		return all_data,profile_dict

	def get_event_logs(self,all_action_data):

		query = 'SELECT id as profile_id,agent_id FROM agent_profiles WHERE account_id={}'.format(self.account['id'])
		self.cur.execute(query)
		agent_df = pd.DataFrame(self.cur.fetchall())
		col_name = [x[0] for x in self.cur.description]
		agent_df = agent_df.rename(columns={iii:col for iii,col in enumerate(col_name)})

		user_id = []
		lead_id = []
		entry_id = []
		created_at = []
		action_type = []
		campaign_name = []
		call_type = []
		to_log = []
		from_log = []
		
		known_data_keys = ['to','from','call_type','campaign_name']
		
		#############################
		# Find all 'data' child keys
		#############################
		secure_log('Find Data Keys')
		data_keys = []
		#for action in df2['actions']:
		for action in all_action_data:
			action = str(action)
			action_filter = action.replace('[','').replace(']','').replace("'","\"").replace('u"','"').replace('None','null')
		
			if (action_filter != ''):
				Nevents = [m.start()+1 for m in re.finditer('}, {', action_filter)]
		
				if (len(Nevents) == 0):
					cur_json = json.loads(action_filter)
					data_keys = data_keys + list(cur_json['data'].keys())
				else:
					Nevents = [-2] + Nevents + [len(action_filter)]
					for iii in range(0,len(Nevents)-1):
						cur_json = json.loads(action_filter[(Nevents[iii]+2):Nevents[iii+1]])
						data_keys = data_keys + list(cur_json['data'].keys())
		
				data_keys = list(set(data_keys))
		
		for cur_key in data_keys:
			if (cur_key not in known_data_keys):
				secure_log('New Ricochet Data Key Found: {}'.format(cur_key))
		
		#########################
		# Populate all log data
		#########################
		secure_log('Scrape all log data')
		total_events = 0
		for action in all_action_data:
			action = str(action)
			action_filter = action.replace('[','').replace(']','').replace("'","\"").replace('u"','"').replace('None','null')
		
			if (action_filter != ''):
				Nevents = [m.start()+1 for m in re.finditer('}, {', action_filter)]
		
				if (len(Nevents) == 0):
					cur_json = json.loads(action_filter)
					user_id.append(cur_json['user_id'])
					lead_id.append(cur_json['lead_id'])
					entry_id.append(cur_json['entry_id'])
					action_type.append(cur_json['action_type'])
					created_at.append(cur_json['created_at'])
					cur_json_data = cur_json['data']
					if ('to' in cur_json_data.keys()):
						to_log.append(cur_json_data['to'])
					else:
						to_log.append(None)
					if ('from' in cur_json_data.keys()):
						from_log.append(cur_json_data['from'])
					else:
						from_log.append(None)
					if ('call_type' in cur_json_data.keys()):
						call_type.append(cur_json_data['call_type'])
					else:
						call_type.append(None)
					if ('campaign_name' in cur_json_data.keys()):
						campaign_name.append(cur_json_data['campaign_name'])
					else:
						campaign_name.append(None)
		
					total_events = total_events + 1
				else:
					Nevents = [-2] + Nevents + [len(action_filter)]
					for iii in range(0,len(Nevents)-1):
						cur_json = json.loads(action_filter[(Nevents[iii]+2):Nevents[iii+1]])
						user_id.append(cur_json['user_id'])
						lead_id.append(cur_json['lead_id'])
						entry_id.append(cur_json['entry_id'])
						action_type.append(cur_json['action_type'])
						created_at.append(cur_json['created_at'])
						cur_json_data = cur_json['data']
						if ('to' in cur_json_data.keys()):
							to_log.append(cur_json_data['to'])
						else:
							to_log.append(None)
						if ('from' in cur_json_data.keys()):
							from_log.append(cur_json_data['from'])
						else:
							from_log.append(None)
						if ('call_type' in cur_json_data.keys()):
							call_type.append(cur_json_data['call_type'])
						else:
							call_type.append(None)
						if ('campaign_name' in cur_json_data.keys()):
							campaign_name.append(cur_json_data['campaign_name'])
						else:
							campaign_name.append(None)
		
						total_events = total_events + 1
		
		secure_log('Total Events  = {}'.format(total_events))
		secure_log('user_id	   = {}'.format(len(user_id)))
		secure_log('lead_id	   = {}'.format(len(lead_id)))
		secure_log('entry_id	  = {}'.format(len(entry_id)))
		secure_log('action_type   = {}'.format(len(action_type)))
		secure_log('created_at	= {}'.format(len(created_at)))
		secure_log('to_log		= {}'.format(len(to_log)))
		secure_log('from_log	  = {}'.format(len(from_log)))
		secure_log('call_type	 = {}'.format(len(call_type)))
		secure_log('campaign_name = {}'.format(len(campaign_name)))
		
		df = pd.DataFrame()
		df['user_id'] = user_id
		df['lead_id'] = lead_id
		df['entry_id'] = entry_id
		df['action_type'] = action_type
		df['created_at'] = created_at
		df['to_log'] = to_log
		df['from_log'] = from_log
		df['call_type'] = call_type
		df['campaign_name'] = campaign_name
		
		df['user_id'] = df['user_id'].astype('Int64')
		df['created_at'] = pd.to_datetime(df['created_at'],format = '%m/%d/%Y %H:%M:%S %p')
		
		df = df.sort_values(['lead_id','created_at']).reset_index(drop=True)
		
		###########################################
		# Insert missing agents into agent_profiles
		###########################################
		missing_agents = list(sorted(set([x for x in list(df['user_id']) if pd.isna(x) == False and x not in list(agent_df['agent_id'])])))
		missing_agents  = [x for x in missing_agents if pd.isnull(x) == False]
		secure_log('Missing Agent Ids . {}'.format(missing_agents))	

		# Insert missing agent_ids in agent_profiles
		if (len(missing_agents) > 0):
			query = 'SELECT max(id) as max_id FROM agent_profiles'
			self.cur.execute(query)
			max_id = int(self.cur.fetchall()[0][0])

			query = "SELECT id,name FROM agent_profiles WHERE account_id = {} AND name LIKE 'Removed Agent%'".format(self.account['id'])
			self.cur.execute(query)
			removed_df = pd.DataFrame(self.cur.fetchall())
			col_name = [x[0] for x in self.cur.description]
			removed_df = removed_df.rename(columns={iii:col for iii,col in enumerate(col_name)})

			removed_df['max_int'] = [int(x.replace('Removed Agent','')) for x in removed_df['name']]

			max_removed_id = int(max(removed_df['max_int']))
			for agent in missing_agents:
				if (isinstance(agent,np.int64) == True):
					max_removed_id = max_removed_id + 1
					max_id = max_id + 1
					secure_log('INSERT into agent_profiles ... account_id,agent_id . {},{}'.format(self.account['id'],agent))
					query = """INSERT INTO agent_profiles(id, account_id, agent_id, name, name_velocify, first_name, last_name, created_at, updated_at)
							VALUES
							($${}$$,$${}$$,$${}$$,$$Removed Agent{}$$,$$Agent{}, Removed$$,$$Removed$$,$$Agent{}$$,$${}$$,$${}$$)"""\
							.format(max_id,self.account['id'],agent,str(max_removed_id).zfill(3),str(max_removed_id).zfill(3),str(max_removed_id).zfill(3),
									dt.datetime.now(),dt.datetime.now())
					self.cur.execute(query)
					self.con.commit()

			# Get new data
			query = 'SELECT id as profile_id,agent_id FROM agent_profiles WHERE account_id={}'.format(self.account['id'])
			self.cur.execute(query)
			agent_df = pd.DataFrame(self.cur.fetchall())
			col_name = [x[0] for x in self.cur.description]
			agent_df = agent_df.rename(columns={iii:col for iii,col in enumerate(col_name)})

		############################
		# Find 1st assignment
		############################
		secure_log('Find FA for each lead')
		fa_all_df = df[pd.isnull(df['user_id']) == False].groupby('lead_id').first().reset_index().rename(columns={'user_id':'agent_id_first_assignment_user'})
		fa_all_df = pd.merge(fa_all_df,agent_df.rename(columns={'profile_id':'profile_id_first_assignment_user'}),'left',left_on='agent_id_first_assignment_user',right_on='agent_id')
		
		# 1) Grab 1st LO that is NOT admin ... profile_id_first_assignment_user
		fa_df = df[(pd.isnull(df['user_id']) == False) & (df['user_id'] != 43007753)].groupby('lead_id').first().reset_index().rename(columns={'user_id':'agent_id_first_assignment_user'})
		fa_df = pd.merge(fa_df,agent_df.rename(columns={'profile_id':'profile_id_first_assignment_user'}),'left',left_on='agent_id_first_assignment_user',right_on='agent_id')

		###############################################################
		# profile_id_loan_officer_default is 
		# 1) profile_id_first_assignment_user by default
		# 2) closing LO if iclose = 1 . this reset comes in lockclose
		###############################################################
		fa_df['profile_id_loan_officer_default'] = fa_df['profile_id_first_assignment_user']
		
		#####################################################################################################	
		# Add fa_all_df to fa_df if it does not exist in fa_df ... ie add admin as FA if no other LO appears
		#####################################################################################################	
		secure_log('All First Assignments	   . {}'.format(len(fa_all_df)))
		secure_log('Non-Admin First Assignments . {}'.format(len(fa_df)))

		df_list = []
		df_list.append(fa_df)
		df_list.append(fa_all_df[fa_all_df['lead_id'].isin(list(fa_df['lead_id'])) == False])
		fa_df = pd.concat(df_list,ignore_index=True,sort=True).reset_index(drop=True)

		secure_log('Admin First Assignments			 . {}'.format(len(fa_df[fa_df['agent_id_first_assignment_user'] == 43007753])))
		secure_log('All (Admin Added) First Assignments . {}'.format(len(fa_df)))
	
		#####################################################################################################	
		# 2) Grab final LO that is NOT admin ... profile_id_loan_officer_default
		#####################################################################################################	
		user_all_df = df[(pd.isnull(df['user_id']) == False)].groupby('lead_id').last().reset_index().rename(columns={'user_id':'agent_id_user'})
		user_all_df = pd.merge(user_all_df,agent_df.rename(columns={'profile_id':'profile_id_user'}),'left',left_on='agent_id_user',right_on='agent_id')
		user_all_df['profile_id_loan_officer_default'] = None 
		user_df = df[(pd.isnull(df['user_id']) == False) & (df['user_id'] != 43007753)].groupby('lead_id').last().reset_index().rename(columns={'user_id':'agent_id_user'})
		user_df = pd.merge(user_df,agent_df.rename(columns={'profile_id':'profile_id_user'}),'left',left_on='agent_id_user',right_on='agent_id')

		#####################################################################################################	
		# Add user_all_df to user_df if it does not exist in user_df ... ie add admin as FA if no other LO appears
		#####################################################################################################	
		secure_log('All Users	   . {}'.format(len(user_all_df)))
		secure_log('Non-Admin Users . {}'.format(len(user_df)))

		df_list = []
		df_list.append(user_df)
		df_list.append(user_all_df[user_all_df['lead_id'].isin(list(user_df['lead_id'])) == False])
		user_df = pd.concat(df_list,ignore_index=True,sort=True).reset_index(drop=True)

		secure_log('All (Admin Added) Users . {}'.format(len(user_df)))
	
		# 3) Summary Statistics
		#secure_log(fa_all_df)
		fa_all_count_df = fa_all_df[['lead_id','agent_id_first_assignment_user']].groupby('agent_id_first_assignment_user').count().reset_index().rename(columns={'lead_id':'Nfirst'})
		fa_count_df = fa_df[['lead_id','agent_id_first_assignment_user']].groupby('agent_id_first_assignment_user').count().reset_index().rename(columns={'lead_id':'Nfa'})
		user_all_count_df = user_all_df[['lead_id','agent_id_user']].groupby('agent_id_user').count().reset_index().rename(columns={'lead_id':'Nuser'})
		user_count_df = user_df[['lead_id','agent_id_user']].groupby('agent_id_user').count().reset_index().rename(columns={'lead_id':'Nuser'})
		
		# 4) Add agents to original file
		df = pd.merge(df,fa_df[['lead_id','agent_id_first_assignment_user','profile_id_first_assignment_user']],'left',left_on='lead_id',right_on='lead_id')
		df = pd.merge(df,user_df[['lead_id','agent_id_user','profile_id_user','profile_id_loan_officer_default']],'left',left_on='lead_id',right_on='lead_id')
		
		df['agent_id_first_assignment_user'] = df['agent_id_first_assignment_user'].astype('Int64')
		df['profile_id_first_assignment_user'] = df['profile_id_first_assignment_user'].astype('Int64')
		df['profile_id_user'] = df['profile_id_user'].astype('Int64')
		df['profile_id_loan_officer_default'] = df['profile_id_loan_officer_default'].astype('Int64')
	
		#print(df[(pd.isnull(df['profile_id_user']) == True)]\
		#			[['agent_id_first_assignment_user','profile_id_first_assignment_user','profile_id_user','lead_id']])
		#print(fa_all_count_df)
		#print(fa_count_df)
		#print(df[(pd.isnull(df['profile_id_user']) == True) & (pd.isnull(df['profile_id_first_assignment_user']) == False)]\
		#			[['agent_id_first_assignment_user','profile_id_first_assignment_user','profile_id_user','lead_id']])
	
		# 5) Check global statistics
		moved_leads_df = df[['agent_id_first_assignment_user','profile_id_first_assignment_user','profile_id_user','lead_id']]\
								.groupby(['agent_id_first_assignment_user','profile_id_first_assignment_user','profile_id_user']).count()\
								.reset_index().rename(columns={'lead_id':'Nlead'})
		
		#secure_log(first_count_df)
		#secure_log(fa_count_df)
		#secure_log(moved_leads_df)
		
		both_df = pd.merge(fa_all_count_df,fa_count_df,'left',left_on='agent_id_first_assignment_user',right_on='agent_id_first_assignment_user')
		both_df['Nfirst'] = both_df['Nfirst'].astype('Int64')
		both_df['Nfa'] = both_df['Nfa'].astype('Int64')
		both_df['agent_id_first_assignment_user'] = both_df['agent_id_first_assignment_user'].astype('Int64')
		
		both_df = pd.merge(both_df,user_count_df,'left',left_on='agent_id_first_assignment_user',right_on='agent_id_user')
		both_df['agent_id_user'] = both_df['agent_id_user'].astype('Int64')
		both_df['Nuser'] = both_df['Nuser'].astype('Int64')
		
		both_df = pd.merge(both_df,agent_df,'left',left_on='agent_id_first_assignment_user',right_on='agent_id')
		both_df['agent_id'] = both_df['agent_id'].astype('Int64')
		both_df['profile_id'] = both_df['profile_id'].astype('Int64')
	
		both_df = both_df[['agent_id_first_assignment_user','agent_id_user','Nfirst','Nfa','Nuser']] 
		secure_log('\n{}'.format(both_df))

		####################################
		# Only return 1 value for each lead	
		####################################
		df = df[(pd.isnull(df['agent_id_first_assignment_user']) == False) | ((pd.isnull(df['agent_id_first_assignment_user']) == False) & (pd.isnull(df['agent_id_user']) == True))].copy(deep=True)
		profile_id_df = df[['lead_id','profile_id_first_assignment_user','profile_id_user','profile_id_loan_officer_default']].groupby(['lead_id']).first()

		return profile_id_df[['profile_id_first_assignment_user','profile_id_user','profile_id_loan_officer_default']].to_dict('index')
	
