import requests 
import xmltodict

from utils import group_by_column

class Velocify(object):
	def __init__(self, username, password):
		self.username = username
		self.password = password

	def group_by_column(self, data, column):
		result = {}
		for item in data:
			if(item[column] not in result):
				result[item[column]] = []
			result[item[column]].append(item)

		return result

	def get_fields(self):
		url	= "https://service.leads360.com/ClientService.asmx/GetFields"
		payload = {'username': self.username, 'password': self.password}

		# sending get request and saving the response as response object
		r = requests.post(url, data = payload)
		try:
			data = xmltodict.parse(r.content, attr_prefix='', dict_constructor=dict)
			return data['Fields']['Field']
		except Exception as e:
			secure_log('xmltodict failure . {}'.format(e))
			return None

	def get_campaigns(self):
		url	= "https://service.leads360.com/ClientService.asmx/GetCampaigns"
		payload = {'username': self.username, 'password': self.password}

		# sending get request and saving the response as response object
		r = requests.post(url, data = payload)
		try:
			data = xmltodict.parse(r.content, attr_prefix='', dict_constructor=dict)
			return group_by_column(data['Campaigns']['Campaign'], 'Note')
		except Exception as e:
			secure_log('xmltodict failure . {}'.format(e))
			return None
