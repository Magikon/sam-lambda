import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import json
import re
import dateutil.parser
import datetime
from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro

# Adding aboslute path for different paths
_CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT_FOLDER = os.path.dirname(_CURRENT_DIR)

sys.path.append(os.path.join(_ROOT_FOLDER, 'pplibs'))
from kafka_utils import get_kafka_config, fetch_kafka_pass

long=int

	
class KafkaAvroProducer:
	__config = None
	__k_pass = None 
	__producer = None 

	def __init__(self, topic, key = None):
		KafkaAvroProducer.__config = KafkaAvroProducer.__config if KafkaAvroProducer.__config else get_kafka_config()
		KafkaAvroProducer.__k_pass = KafkaAvroProducer.__k_pass if KafkaAvroProducer.__k_pass else fetch_kafka_pass()

		self.config = KafkaAvroProducer.__config
		self.producer = self.get_producer()

		print("::::: Initiating {} avro producer".format(topic))
		self.topic = topic
		self.schema_dict = self.fetch_schema()
		self.value_schema = avro.loads(json.dumps(self.schema_dict))

		if key is not None:
			key_schema_string = """
			{"type": "{}"}
			""".format(type(key))
			self.key_schema = avro.loads(key_schema_string)

	def parse_record(self, record):
		parsed_record = {}

		# Standardize record shared_keys
		for key in record.keys():
			record[key.lower()] = record[key]

		for col in self.schema_dict['fields']:
			if col['name'] in record:
				
				if (col['type'][0] in ['long', 'int']):
					test_val = record[col['name']]

					if (isinstance(test_val, str)):
						if re.search(r"[^\d.\s]", test_val):
							try:
								dt = dateutil.parser.parse(test_val)
								test_val = int(dt.timestamp() * 1000000)
							except:
								pass
					try:
						test_val = int(test_val)
					except:
						test_val = None

					parsed_record[col['name']] = test_val
				elif (col['type'][0] in ['float', 'double']):
					try:
						test_val = float(record[col['name']])
					except:
						test_val = None
					
					parsed_record[col['name']] = test_val
				else:
					val = record[col['name']]
					parsed_record[col['name']] = str(val) if val is not None else None
			else:
				val = None
				if col['name'] in ['created_at', 'updated_at']:
					val = int(datetime.datetime.now().timestamp() * 1000000)
				parsed_record[col['name']] = val

		return parsed_record

	def get_producer(self):
		if KafkaAvroProducer.__producer:
			return KafkaAvroProducer.__producer
		else:
			config = {
				"security.protocol": 'SASL_PLAINTEXT',
				"bootstrap.servers": self.config['host'],
				"schema.registry.url": self.config['schema_host'],
				"sasl.mechanism": 'PLAIN',
				"sasl.username": self.config["user"],
				"sasl.password": KafkaAvroProducer.__k_pass,
			}

			return AvroProducer(config)

	def send(self, message, key = None):

		parsed_message = self.parse_record(message)

		if key is None:
			self.producer.produce(topic=self.topic, value=parsed_message, value_schema=self.value_schema)
		else:
			key_schema_string = self.get_key_schema(key)

			self.producer.produce(topic=self.topic, key=key, value=parsed_message, key_schema=avro.loads(key_schema_string), value_schema=self.value_schema)
	
	def get_key_schema(self, key):
		if isinstance(key, int):
			return """
			{"type": "long"}
			"""
		elif isinstance(key, float):
			return """
			{"type": "double"}
			"""
		elif isinstance(key, str):
			return """
			{"type": "string"}
			"""
		else:
			raise Exception("AvroProducer Exception! Key data type not supported! {}".format(type(key)))

	
	def flush(self):
		self.producer.flush()
		
	def fetch_schema(self):
		print (f"::::: Fetching ksql schema for table {self.topic}")
		r = requests.get(f"{self.config['schema_host']}/subjects/{self.topic}-value/versions")

		schema_versions = json.loads(r.content)
		if 'error_code' in schema_versions:
			raise Exception(f"AvroProducer Exception! {schema_versions['message']}")
		else:
			print (f"::::: Schema Found! Latest version: {max(schema_versions)}")
			r = requests.get(f"{self.config['schema_host']}/subjects/{self.topic}-value/versions/{max(schema_versions)}")

			return json.loads(json.loads(r.content)['schema'])
