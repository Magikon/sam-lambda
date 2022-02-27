from faker import Faker
import os
import math
import re
import random
import xmltodict
import psycopg2
import requests 
import boto3
from datetime import datetime, timedelta

fake = Faker()

from helpers.velocify import Velocify
from helpers.lookups import Lookups
from helpers.data_generator import DataGenerator

from utils import group_by_column
from utils import meld_columns

from pplibs.logUtils import get_account_config

bad_counter = 0

class LeadGenerator(object):

	def __init__(self, cur):
		
		self.cur = cur
		self.accounts = self.get_accounts()
		self.config = get_account_config(1, cache=False)['lead_generation']
		self.velocify = Velocify(self.accounts[list(self.accounts)[0]]['velocify_username'], self.accounts[list(self.accounts)[0]]['velocify_password'])
		self.campaigns = self.velocify.get_campaigns()
		self.generator = DataGenerator()
		
		self.lookups = self.get_lookups()
		self.xml_map = self.build_xml_map()


	def get_accounts(self):
		print("::::: Fetching Accounts for Lead Generation")
		sql = """
			SELECT id, name, velocify_username, velocify_password from accounts where id <> 10 and token is not NULL and velocify_password is not NULL;
		"""
		self.cur.execute(sql)

		data = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])

		result = {}
		for item in data:
			result[item['name']] = item

		return result

	def generate_field(self, bad_data, missing_data, account, value):
		if (bad_data and bool(random.random() < float(self.config['accounts'][account]['bad_data_prob']))): #Probability of generating bad_data
			global bad_counter 
			bad_counter += 1
			if(isinstance(value, str)):
				if (bool(random.random() < 0.4)): #Probability of inserting unicode
					return self.generator.insert_unicode(value, random.randint(1,2))
				else:
					return self.generator.add_random_char(value)
			elif(isinstance(value, int)):
				if (bool(random.random() < 0.7)): #Probability of increasing int size
					return value + random.randint(2000000000,3147483647)
				else:
					return self.generator.add_random_char(str(value))
			else:
				return value
		elif (missing_data and bool(random.random() < float(self.config['accounts'][account]['missing_data_prob']))): #Probability of returning nothing
			return ''
		else:
			return value

	def get_template(self, account):

		price = random.randint(100000, 2000000)
		loan = random.randint(50000, int(price * 0.8))
		home = random.randint(100000, 2000000)
		down = int(price - loan)
		zipc = str(random.randint(35801, 82941))

		use_bad_data = bool(random.random() < float(self.config['accounts'][account]['bad_data_prob']))
		use_missing_data = bool(random.random() < float(self.config['accounts'][account]['missing_data_prob']))
		
		lead_template = {
			"CustomerId": self.accounts[account]['id'],
			"FirstName": self.generate_field(use_bad_data, use_missing_data, account, fake.first_name()),
			"LastName": self.generate_field(use_bad_data, use_missing_data, account, fake.last_name()),
			"Email": self.generate_field(use_bad_data, use_missing_data, account, fake.email()),
			"DayPhone": self.generate_field(use_bad_data, use_missing_data, account, self.generator.rand_phone()),
			"EveningPhone": self.generate_field(use_bad_data, use_missing_data, account, self.generator.rand_phone()),
			"PurchasePrice":  self.generator.format_currency(self.generate_field(use_bad_data, use_missing_data, account, price)),
			"LoanAmount":  self.generator.format_currency(self.generate_field(use_bad_data, use_missing_data, account, loan)),
			"DownPayment": self.generator.format_currency(self.generate_field(use_bad_data, use_missing_data, account, down)),
			"downPaymentStated": self.generator.format_currency(self.generate_field(use_bad_data, use_missing_data, account, down)),
			"ExistingHomeValue":  self.generator.format_currency(self.generate_field(use_bad_data, use_missing_data, account, home)),
			"PropertyAddress": "na",
			"PropertyZipCode" : zipc,
			"ZipCode": zipc,
			"IsMilitary": str(fake.boolean()),
			"PropertyState": random.choice(self.lookups['property_states'][account])['property_state_stated'],
			"PropertyType": random.choice(self.lookups['property_types'][account])['property_type_stated'],
			"IntendedPropertyUse": random.choice(self.lookups['property_uses'][account])['property_use_stated'],
			"LoanPurpose": random.choice(self.lookups['purpose_details'][account])['purpose_detail_stated']
		}

		lead_template.update(self.get_extra_fields(account, lead_template, use_bad_data, use_missing_data))

		return lead_template

	def generate_leads(self, account, amount = 1, start_dt = None, end_dt = None, max_interval=60):
		global bad_counter
		leads = []

		if(start_dt and end_dt):
			print("::::: Generating Leads from {} to {} for Account: {}".format(start_dt, end_dt, account))
			id_count = -1
			while start_dt < end_dt:
				lead = self.get_template(account)
				lead['Id'] = id_count
				lead['DateAdded'] = start_dt.strftime('%Y-%m-%dT%H:%M:%S')
				leads.append(lead)
				
				rand_m = random.randint(1,max_interval)
				start_dt += timedelta(minutes=rand_m)
				if (start_dt.hour < 8):
					start_dt = start_dt + timedelta(hours=(8 - start_dt.hour))

				id_count -= 1
			print("BAD" + str(bad_counter))
			return leads
		elif (amount == 1):
			print("::::: Generating a lead for Account: {}".format(account))
			lead = self.get_template(account)
			return lead 
		else:
			print("::::: Generating {} Leads for Account: {}".format(amount, account))
			for i in range(0, amount):
				lead = self.get_template(account)
				leads.append(lead)
			
			return leads

	def get_lookups(self):
		print("::::: Fetching Lookups")

		ls = Lookups(self.cur, self.accounts)
		result = {}
		result['global_attr'] = ls.get_global_attr('xml_post')
		result['xml_global_attr'] = ls.get_global_attr(['xml_post', 'lead_detail', 'lead'])
		result['source_details'] = ls.get_source_details()
		result['credit_profiles'] = ls.get_credit_profiles()
		result['property_types'] = ls.get_property_types()
		result['property_states'] = ls.get_property_states()
		result['property_uses'] = ls.get_property_uses()
		result['purpose_details'] = ls.get_purpose_details()

		return result

	def build_xml_map(self):
		map_attr = self.lookups['xml_global_attr']
		
		result_map = {}
		for account in map_attr:
			#Merge items based on propair_field
			attr = {}
			for item in map_attr[account]:
				if item['propair_field'] not in attr:
					attr[item['propair_field']] = {}

				attr[item['propair_field']][item['table_name']] = item['customer_field_name']
			
			item_map = {}
			#Retrieve xml_post mapping
			for x, y in {key: attr[key] for key in attr if 'xml_post' in attr[key]}.items():
				value = y.pop('xml_post').split(':')[0]
				if y != {}:
					field = next(iter(y.values())).split(':')[0]
					item_map[field] = value

			result_map[account] = item_map

		return result_map

	def convert_to_post(self, lead, account):
		lead_post = {}
		default_map = {
			'FICO': 'FICO',
			'CustomerId': 'CustomerId',
			'ActiveProspect': 'ActiveProspect'
		}
		for key in lead:
			if key in self.xml_map[account]:
				lead_post[self.xml_map[account][key]] = lead[key]
			elif key in default_map:
				lead_post[default_map[key]] = lead[key]
			else:
				new_key = key[0].lower() + key[1:]
				lead_post[new_key] = lead[key]

		return lead_post

	def get_extra_fields(self, account, lead, use_bad_data, use_missing_data):
		extra_fields = {}
		
		if (account in self.campaigns.keys()):
			campaign_set = random.choice(self.campaigns[account])
			extra_fields['CampaignId'] = campaign_set['CampaignId']
			extra_fields['LeadSource'] = campaign_set['CampaignTitle']
			extra_fields['LeadSourceGroup'] = campaign_set['CampaignGroupTitle'] if 'CampaignGroupTitle' in campaign_set else ''

			campaign = campaign_set['CampaignTitle']
			campaign_group = campaign_set['CampaignGroupTitle'] if 'CampaignGroupTitle' in campaign_set else '3RDPARTY'


			
			source_details = self.lookups['source_details'][account] 

			for mapping in [x for x in self.lookups['global_attr'][account] if x['customer_field_name'] != None]:			
				if re.sub("[^a-z_A-Z]", "", mapping['customer_field_name']) in ['campaign', 'lead_source']:
					if (":" in mapping['customer_field_name']):
						if (mapping['propair_field'] in source_details[0].keys()):
							split = mapping['customer_field_name'].split(":")
							field = split[0]
							index = int(split[1])

							campaign_split = campaign.upper().split(mapping['customer_split'])
							value = campaign_split[index] if 0 <= index < len(campaign_split) else 'MISSING'
							filt = [x for x in source_details if x[mapping['propair_field']] == value]
							if (len(filt) > 0):
								source_details = filt
					else:
						filt = [x for x in source_details if x[mapping['propair_field']] == re.sub("[^0-9a-zA-Z]", "", campaign.upper())]
						if (len(filt) > 0):
							source_details = filt
				if mapping['customer_field_name'] in ['campaignGroup', 'lead_source_group']:
					filt = [x for x in source_details if x[mapping['propair_field']] == re.sub("[^0-9a-zA-Z]", "", campaign_group.upper())]
					if (len(filt) > 0):
						source_details = filt
			
			source_details = random.choice(source_details)
			credit_profile = random.choice(self.lookups['credit_profiles'][account][source_details['source']]) if source_details['source'] in self.lookups['credit_profiles'][account] else None
			extra_fields['CreditProfile'] = credit_profile['credit_profile_stated'] if credit_profile else ''

			for mapping in [x for x in self.lookups['global_attr'][account] if x['customer_field_name'] != None]:
				if (':' not in mapping['customer_field_name'] and mapping['customer_field_name'].upper() not in [x.upper() for x in list(lead.keys())] + [x.upper() for x in list(extra_fields.keys())] + ['CAMPAIGN', 'CAMPAIGNGROUP', 'ID'] and mapping['qa_required'] == 'yes'):
					if(mapping['propair_field'] == 'source'):
						extra_fields[mapping['customer_field_name']] = source_details['source'] if source_details['source'] != 'MISSING' else ''
					elif(mapping['propair_field'] == 'source_detail'):
						extra_fields[mapping['customer_field_name']] = source_details['source_detail'] if source_details['source'] != 'MISSING' else ''
					elif(mapping['propair_field'] == 'campaign_group'):
						extra_fields[mapping['customer_field_name']] = source_details['campaign_group'] if source_details['source'] != '3RDPARTY' else ''
					else:
						extra_fields[mapping['customer_field_name']] = self.generate_field(use_bad_data, use_missing_data, account, self.generator.generate_data_by_type(mapping['datatype']))

			return extra_fields
		else:
			print(":::::: Error! No campaigns found for account: {}".format(account))

