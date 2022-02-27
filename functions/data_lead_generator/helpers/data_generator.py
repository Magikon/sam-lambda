import random 
from faker import Faker 
fake = Faker()

class DataGenerator(object):

	def __init__(self):
		self.bad_chars = [
			"e",
			"'",
			"+",
			"Down"
		]
		print("Initiated data generator")
		#something

	def generate_data_by_type(self,datatype):
		if (datatype == 'score'):
			return random.randint(300,900)
		elif (datatype == 'bool'):
			return str(fake.boolean())
		else: 
			return None
		

	def rand_phone(self): 
		range_start = 10**(7)
		range_end = (10**8)-1
		n = str(random.randint(range_start, range_end))
		return '555-' + n[1:4] + '-' + n[4:]

	def format_currency(self, value):
		if(isinstance(value, int)):
			return '${:,.2f}'.format(value)
		else:
			return value

	def add_random_char(self, value, amount = 1):
		if (isinstance(value, str) and value != ''):
			for i in range(0, amount):
				rand_char = random.choice(self.bad_chars) # Set random unicode character
				rand_position = random.randint(0, len(value)-1) # Choose random position in value
				value = value[:rand_position] + rand_char + value[rand_position:]  

			return value
		else:
			return value

	def insert_unicode(self, value, amount = 1):
		if (isinstance(value, str) and value != ''):
			for i in range(0, amount):
				rand_unicod = random.randint(0,1114110) # Set random unicode character
				rand_position = random.randint(0, len(value)-1) # Choose random position in value
				value = value[:rand_position] + chr(rand_unicod) + value[rand_position+1:]  

			return value
		else:
			return value
