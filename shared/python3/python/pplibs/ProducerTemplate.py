from time import sleep
from json import dumps
from kafka import KafkaProducer
import yaml 
import random

class ProducerTemplate(object):

	def __init__(self, config_path):
		self.config = yaml.load( open(config_path))

		self.host= self.config["host"]
		self.topic= self.config["topic"]

		self.producer = KafkaProducer(bootstrap_servers=[self.host],
								 compression_type='snappy',
								 acks= self.config["acks"],
								 retries= self.config["retries"],
								 security_protocol='SASL_PLAINTEXT',
								 sasl_mechanism='PLAIN',
								 sasl_plain_username=self.config["user"],
								 sasl_plain_password=self.config["password"],
								 linger_ms= self.config['linger_ms'],
								 batch_size= 32*1024 ,
		                         value_serializer=lambda x: dumps(x).encode('utf-8')
		                         )

		self.success = 0
		self.fail = 0
		self.offset = 0
		print("::: Producer created :::")


	def on_send_success(self, record_metadata):
		self.success += 1
		self.offset += record_metadata.offset
		# print('{0} {1}  {2}'.format(record_metadata.topic,record_metadata.partition, record_metadata.offset) )

	def on_send_error(self, excp):
		self.fail += 1
		log.error('Something went wrong', exc_info=excp)

	def send(self, message):
		data = message
		k = bytes( str(random.randint(1,1000000)).encode("utf-8") )
		p = self.producer.send(self.config["topic"], key=k, value=data).\
			add_callback(self.on_send_success).\
			add_errback(self.on_send_error)

		# block until all sync messages are sent
		self.producer.flush()
		return {"success": self.success, "fail": self.fail, "offset": self.offset}
  
	def close(self):
		self.producer.close()
