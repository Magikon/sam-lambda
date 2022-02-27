from utils import meld_columns
from utils import group_by_column

class Lookups(object):
	
	def __init__(self, cur, accounts):
		self.cur = cur
		self.accounts = accounts

	def group_by_account(self, data):
		result = {}
		for account in self.accounts.values():
			result[account['name']] = [x for x in data if x['account_id'] == account['id']]

		return result

	def get_global_attr(self, table, include_variable = False):
		print("::::: Fetching Global Attribute Lookup")
		include_variable_string = "AND include_variable = 1" if include_variable == True else ""

		sql = """
			select
				account_id,
				propair_field,
				datatype,
				table_name,
				customer_field_name,
				customer_original_field_name,
				customer_split,
				qa_required
			from
				global_attribute_lookup
			where table_name IN ({})
			and ( customer_field_name IS NOT NULL OR customer_original_field_name IS NOT NULL OR  customer_split IS NOT NULL )
			{}
			order by
				account_id, customer_field_name;
		""".format(
			','.join(['$${}$$'.format(x) for x in list(table)]) if isinstance(table, list) else '$${}$$'.format(table)
			, include_variable_string
			)
			
		self.cur.execute(sql)

		data = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])

		return self.group_by_account(data)


	def get_source_details(self):
		print("::::: Fetching Source Detail Lookup")
		sql = """
			select
				account_id,
				source,
				source_detail,
				campaign_group
			from source_detail_lookup
			order by
				account_id, source, source_detail, campaign_group;
		"""
		self.cur.execute(sql)

		data = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])

		grouped_fields = self.group_by_account(data)

		#custom cardinal fields
		#cardinal_campaign_groups = {
		#	'CCDLIVETRANSFER': {'source_description': 'CCD', 'source_division2': 'Live Transfer'},
		#	'CCDDATALEAD': {'source_description': 'CCD', 'source_division2': 'Data Lead'},
		#	'CCDLTLFCOB': {'source_description': 'CCD', 'source_division2': 'LTLF COB'},
		#	'SEBONIC1OUTBOUND': {'source_description': 'Sebonic 1', 'source_division2': 'Outbound'}
		#}
		#cardinal_sources = {
		#	'ZILLOW': {'source_division': 'CD'},
		#	'FREERATEUPDATE': {'source_division': 'CFD'},
		#	'LENDGO': {'source_division': 'CFD'},
		#	'LPC': {'source_division': 'CFD'},
		#	'LTLF': {'source_division': 'CFD'},
		#	'CREDITKARMACPL': {'source_division': 'SCD'},
		#}
#
		## Filter Cardinal Campaign Groups
		#cardinal_fields = [x for x in grouped_fields['cardinal'] if x['campaign_group'] in cardinal_campaign_groups.keys()]
		#for item in cardinal_fields:
		#	item.update(cardinal_campaign_groups[item['campaign_group']])
#
		## Filter Cardinal Sources
		#cardinal_fields = [x for x in grouped_fields['cardinal'] if x['campaign_group'] in cardinal_campaign_groups.keys()]
		#for item in cardinal_fields:
		#	item.update(cardinal_campaign_groups[item['campaign_group']])

		#grouped_fields['cardinal'] = cardinal_fields
		return grouped_fields

	
	def get_credit_profiles(self):
		print("::::: Fetching Credit Profile Lookup")
		sql = """
			select
				account_id,
				source,
				credit_profile_stated
			from
				credit_profile_stated_lookup
			order by
				account_id, source;
		"""
		self.cur.execute(sql)

		data = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])

		grouped_fields = self.group_by_account(data)

		for account in grouped_fields:
			account_fields = grouped_fields[account]

			grouped_source_fields = group_by_column(account_fields, 'source')

			grouped_fields[account] = grouped_source_fields

		return grouped_fields

	def get_property_states(self):
		print("::::: Fetching Property State Lookup")
		sql = """
			select 
				account_id,
				property_state_stated
			from
				property_state_stated_lookup
			order by
				account_id, property_state_stated;
		"""
		self.cur.execute(sql)

		data = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])

		return self.group_by_account(data)

	def get_property_types(self):
		print("::::: Fetching Property Type Lookup")
		sql = """
			select
				account_id,
				property_type_stated
			from
				property_type_stated_lookup
			order by
				account_id, property_type_stated;
		"""
		self.cur.execute(sql)

		data = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])

		return self.group_by_account(data)

	def get_property_uses(self):
		print("::::: Fetching Property Use Lookup")
		sql = """
			select
				account_id,
				property_use_stated
			from
				property_use_stated_lookup
			order by
				account_id, property_use_stated;
		"""
		self.cur.execute(sql)

		data = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])

		return self.group_by_account(data)

	def get_purpose_details(self):
		print("::::: Fetching Purpose Detail Lookup")
		sql = """
			select
				account_id,
				purpose_detail_stated,
				"BIN_purpose_detail_stated2"
			from
				purpose_detail_stated_lookup
			order by
				account_id, purpose_detail_stated;
		"""
		self.cur.execute(sql)

		data = meld_columns(self.cur.fetchall(), [x.name for x in self.cur.description])

		return self.group_by_account(data)
	