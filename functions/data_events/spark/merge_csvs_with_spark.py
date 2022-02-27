import boto3
import base64
import os
import re
import glob
#import pandas as pd
import datetime
import dateutil.parser
import psycopg2
from boto3.s3.transfer import TransferConfig
from pyspark import SparkContext
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
import pyspark.sql.functions as f
from pyspark.sql.types import DateType
import sys 
import yaml
import json
from pyspark.sql.types import StringType, IntegerType, DateType
from pyspark.sql.functions import *
#from pyspark.sql.functions import udf
#from pyspark.sql.functions import min, max

#from pplibs.logUtils import secure_log
from dependencies.logUtils import secure_log
from dependencies.logUtils import get_aws_credentials

# load config
config_path = "config.yaml"
config = yaml.load( open(config_path))
# aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
# aws_secret_key = os.environ["AWS_SECRET_KEY"]

# get secrets 
secrets = get_aws_credentials(config)
aws_access_key_id = secrets["AWS_ACCESS_KEY_ID"] 
aws_secret_key = secrets["AWS_SECRET_ACCESS_KEY"]

s3 = boto3.client('s3', 
					  aws_access_key_id=aws_access_key_id, 
					  aws_secret_access_key=aws_secret_key, 
					  region_name=config["aws_region"]
					  )

s3_resource = boto3.resource('s3', aws_access_key_id=aws_access_key_id, \
	aws_secret_access_key=aws_secret_key, \
	region_name=config["aws_region"])	   
 

# Create a spark session
spark = SparkSession.builder.getOrCreate()

csv_directory = './csv'
extension = 'csv'

db_name = config["db_name"] #options["DB_NAME"] #os.environ["DB_NAME"]
db_user = config["db_user"] #options["DB_USER"] #os.environ["DB_USER"]
db_port = 5439
env = config["env"] #options["ENV"] #os.environ["ENV"]
bucket = config["bucket"] #options["BUCKET"] #os.environ["BUCKET"] #options["bucket"]
table_name = config["table_name"] #"external_events"
prefix = config['prefix']
sort_key = config['sort_key']
unique_fields = config['unique_fields']
#### Getting DB credentials
#client = boto3.client('ssm')
client = boto3.client('ssm', 
					  aws_access_key_id=aws_access_key_id, 
					  aws_secret_access_key=aws_secret_key, 
					  region_name=config["aws_region"]
					  )

secure_log("::: env is {} :::".format(env))
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(env), 
	WithDecryption=True
) 

db_pass = response['Parameter']['Value']
db_host = config["db_endpoint"] #options["DB_ENDPOINT"] #os.environ["DB_ENDPOINT"]

#Fetch database schema to apply Int64 to integer types
def mapDataType(row):
	if (row[1] == 'integer'):
		return {'{}'.format(row[0]): 'Int64'}

def remove_old_folder(prefix, file_name):
	_bucket = "pp-tables"
	if env != "staging":
		_bucket = "pp-tables-prod"

	_s3 = s3_resource
	bucket = _s3.Bucket(_bucket)
	objects = bucket.objects.filter(Prefix=prefix).delete()

	# for f in bucket.objects.filter(Prefix='external_events/propair-bank'):
	#	 if file_name in f:
	#		 f.delete()

def parse_date(s_date):
	if s_date is None:
		return ""
	d=s_date
	date_pattern = "%Y-%m-%d %H:%M:%S"
	try:
		mydate = dateutil.parser.parse(s_date)
		d=datetime.datetime.strftime(mydate, date_pattern)
	except:
		pass
	return d

def is_valid_datetime(s_date):
	date_pattern = "%Y-%m-%d %H:%M:%S"
	try:
		mydate = dateutil.parser.parse(s_date)
		return 1		
	except:
		return 0

# def convert_date(s):
# 	date_pattern = "%Y-%m-%d %H:%M:%S"
# 	mydate = str(dateutil.parser.parse(s_date))
#     d = datetime.strftime(mydate, date_pattern)
#     return d

def wrap_in_quotes(any_string):
	if any_string is None:
		return ""
	return '"'+any_string+'"'

def get_all_s3_objects(**base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):  # At the end of the list?
            break
        continuation_token = response.get('NextContinuationToken')

def main():
	wrap_in_quotes_udf = udf(wrap_in_quotes, StringType())
	spark_udf = udf(parse_date, StringType())
	is_valid_datetime_udf = udf(is_valid_datetime, IntegerType())

	secure_log('::: Downloading CSVS :::')
	if os.path.isdir("{}".format(csv_directory)) != True:
		os.mkdir("{}".format(csv_directory))

	try:

		all_customers = s3.list_objects(Bucket = bucket, Prefix='{}/'.format(prefix), Delimiter='/') 
		for o in all_customers['CommonPrefixes']:
			customer = o['Prefix'].split("/")[-2]

			secure_log('Load/Merge All Files From S3')
		
			Nfile = 1 
			for p in get_all_s3_objects(Bucket=bucket, Prefix=o['Prefix']):
				if(p['Key'] != o['Prefix']):
					file_name = p['Key'].split("/")[-1] 
					print(file_name)
					if (Nfile == 1):
						sdf = spark.read.option("delimiter","|").option("escape", "\"").csv('s3n://{}/{}/{}/{}'.format(bucket,prefix,customer,file_name),header=True )
					else:
						sdf = sdf.union(spark.read.option("delimiter","|").option("escape", "\"").csv('s3n://{}/{}/{}/{}'.format(bucket,prefix,customer,file_name),header=True ))

					Nfile += 1

			secure_log('Dataframe Size . (rows,columns) = ({},{})'.format(sdf.count(), len(sdf.columns)))
			secure_log('Get Data Types')

			conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
			cur = conn.cursor()
			sq = "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = '{}'".format(table_name)
			cur.execute(sq)

			## TODO find which dates DO NOT have standard datetime format . find their format and convert to standard datetime
			pg_data = cur.fetchall()
			datetime_types = filter(lambda x: x[1] == 'timestamp without time zone', pg_data) 
			int_types = filter(lambda x: x[1] == 'integer', pg_data) 
			int_types = map(mapDataType, int_types)
			col_datetime_types = [] 
			for d in datetime_types:
				col_datetime_types.append(d[0])

			# Goal is to fix datetime formating issues
			secure_log('Set {} to datetime'.format(col_datetime_types))
			#col_datetime_types = ['modified_at','log_date']
			for cur_col in col_datetime_types:
				sdf = sdf.withColumn(cur_col, spark_udf(cur_col) )
				# sdf = sdf.withColumn(cur_col, from_unixtime(unix_timestamp(cur_col, 'MM/dd/yy HH:mm:ss')))

			secure_log('Sort by {}'.format(sort_key))
			sdf = sdf.orderBy(sort_key)

			# TODO: These commands double the time of execution? ... why?
			secure_log('Drop Duplicates for fields: {}'.format(unique_fields))
			#secure_log('PRE-DROP RECORDS  . {}'.format(sdf.count()))
			sdf = sdf.dropDuplicates(unique_fields)

			secure_log('Post-drop size . (rows) = ({})'.format(sdf.count()))
			# secure_log("::: sdf.show() :::")
			# print(sdf.show())

			# FILTER NON DATETIME ROWS -- Bad Data
			# col_datetime_types = ['log_date', 'created_at', 'modified_at']
			# for curl_col in col_datetime_types:
			# 	sdf = sdf.where( is_valid_datetime_udf( col(cur_col) ) == 1 )
			# secure_log('::: FILTERED NON DATETIME RECORDS :::')

			# RIP OFF DOUBLE QUOTES " FROM LOG_NOTE
			_col = 'log_note'
			# sdf = sdf.withColumn(_col, f.regexp_replace(_col, '\n', ''))
			# secure_log("::: RIP OFF \n FROM LOG_NOTE :::")

			# sdf = sdf.withColumn(_col, wrap_in_quotes_udf( _col ) )
			# secure_log("::: WRAP LOG_NOTE IN DOUBLE QUOTES :::")

			# secure_log('POST-DROP RECORDS . {}'.format(sdf.count()))

			# secure_log('Find Min/Max Dates for Filename')
			# min_date, max_date = sdf.select(min("log_date"), max("log_date")).first()

			#min_date = sdf.agg({"log_date": "min"}).collect()[0]
			#max_date = sdf.agg({"log_date": "max"}).collect()[0]
			#start_date = min_date['min(log_date)'].replace(' ','_')
			#end_date = max_date['max(log_date)'].replace(' ','_')
			#file_name  = "{}_{}_to_{}.csv".format(customer,start_date,end_date)

			file_name  = "{}_{}.csv".format(customer, table_name)
			# secure_log("::: file {} log_date min:{} max:{} ".format(file_name, str(min_date), str(max_date)))

			location = "{0}/{1}/{2}".format(csv_directory, customer, file_name)

			# TODO ... make sure log_subtype_id = NULL is set to -50
#			# fix NaNs
#			secure_log("fixing NaNs ...")
#			df = combined_csv
#
#			for col in df:
#				try: 
#					df[col] += 0
#					df[col].fillna(0, inplace=True)
#				except:
#					df[col].fillna("", inplace=True)
#
			# convert pandas df to spark df 
			file_name = file_name.replace("_","-")
			secure_log("Removing old folders ...")
			remove_old_folder("{}/{}".format(table_name, customer), file_name)

			secure_log("Uploading to S3 ...")
			s3_path = 's3://{}/{}/{}/{}'.format(bucket,table_name,customer, file_name) #,file_name)
			secure_log(s3_path)
			#sdf.write.save(s3_path, format='csv', index=False, mode='overwrite')
			sdf.write.option("escape", "\"").format('com.databricks.spark.csv').save(s3_path, index=False, mode='overwrite')
			#sdf.repartition(1).write.option("escape", "\"").format('com.databricks.spark.csv').save(s3_path, index=False, mode='overwrite')

			secure_log("Successfully uploaded merged csv file to S3 for {}".format(customer))
			secure_log("Location: {}/{}/{}/{}".format(bucket, table_name, customer,file_name))

#			secure_log('Find Min/Max Dates for Spark Dataframe')
#			min_date, max_date = sdf.select(min("log_date"), max("log_date")).first()
#			secure_log("::: log_date min:{} max:{} ".format(str(min_date), str(max_date)))
#			secure_log("::: SPARK DATAFRAME SCHEMA :::")
#			secure_log(sdf.schema.names)
#			secure_log('::: PROCESS COMPLETE :::')


	except Exception as e:
		secure_log("ERROR! {}".format(e))

if __name__ == "__main__":
	main()
