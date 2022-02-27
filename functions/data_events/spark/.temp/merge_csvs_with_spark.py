import boto3
import os
import re
import glob
import pandas as pd
import datetime
import psycopg2
from boto3.s3.transfer import TransferConfig
from pyspark import SparkContext
import pyspark
from pyspark.sql import SparkSession
import sys 
import yaml
#from pplibs.logUtils import secure_log
from dependencies.logUtils import secure_log

# load config
config_path = "config.yaml"
config = yaml.load( open(config_path))
aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
aws_secret_key = os.environ["AWS_SECRET_KEY"]

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
env = config["env"] #options["ENV"] #os.environ["ENV"]
bucket = config["bucket"] #options["BUCKET"] #os.environ["BUCKET"] #options["bucket"]
table_name = config["table_name"] #"external_events"

#### Getting DB credentials
#client = boto3.client('ssm')
client = boto3.client('ssm', 
                      aws_access_key_id=aws_access_key_id, 
                      aws_secret_access_key=aws_secret_key, 
                      region_name=config["aws_region"]
                      )

response = client.get_parameter(
    Name="{}-database-password".format(env), 
    WithDecryption=True
) 

db_pass = response['Parameter']['Value']
db_host = config["db_endpoint"] #options["DB_ENDPOINT"] #os.environ["DB_ENDPOINT"]

def remove_old_folder(prefix, file_name):
    _bucket = "pp-tables"
    if env != "staging":
        _bucket = "pp-tables-prod"

    _s3 = s3_resource
    bucket = _s3.Bucket(_bucket)
    objects = bucket.objects.filter(Prefix=prefix).delete()

    # for f in bucket.objects.filter(Prefix='external_events/propair-bank'):
    #     if file_name in f:
    #         f.delete()

def main():
    secure_log('::: Downloading CSVS :::')
    if os.path.isdir("{}".format(csv_directory)) != True:
        os.mkdir("{}".format(csv_directory))

    try:
        all_customers = s3.list_objects(Bucket = bucket, Prefix='events-tmp/', Delimiter='/') 
        for o in all_customers['CommonPrefixes']:
            customer = o['Prefix'].split("/")[-2]

            #cleaning directory
            initial_filenames = [i for i in glob.glob('{}/{}/*.{}'.format(csv_directory,customer,extension))]
            for f in initial_filenames:
                os.remove(f)

            if os.path.isdir("{}/{}".format(csv_directory, customer)) != True:
                os.mkdir("{}/{}".format(csv_directory, customer))

            all_customer_events = s3.list_objects(Bucket = bucket, Prefix = o['Prefix'])
            for p in all_customer_events['Contents']:
                if(p['Key'] != o['Prefix']):
                    file = p['Key'].split("/")[-1] 
                    if os.path.isfile("{0}/{1}/{2}".format(csv_directory, customer, file)) != True:
                        local_file = "{0}/{1}/{2}".format(csv_directory, customer, file)
                        s3.download_file( bucket, p['Key'], local_file)
                        secure_log("------> File {0} downloaded successfully".format(file))
                    else:
                        secure_log("------> File {0} found, no need to download".format(file))

            all_filenames = [i for i in glob.glob('{}/{}/*.{}'.format(csv_directory,customer,extension))]

            secure_log("Merging csv files..")

            secure_log("Fetching data types..")

            #Fetch database schema to apply Int64 to integer types
            def mapDataType(row):
                if (row[1] == 'integer'):
                    return {'{}'.format(row[0]): 'Int64'}

            conn = psycopg2.connect('host={} dbname={} user={} password={}'.format(db_host, db_name, db_user, db_pass))
            cur = conn.cursor()
            sq = "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = '{}'".format(table_name)

            cur.execute(sq)

            data = cur.fetchall()
            int_types = filter(lambda x: x[1] == 'integer', data) 
            int_types = map(mapDataType, int_types)
            column_types = {}
            for d in int_types:
                column_types.update(d)

            #map(filterDataType, data)
            #combine all files in the list
            combined_csv = pd.concat([pd.read_csv(f, dtype=column_types) for f in all_filenames ])

            #convert log_date to datetime / sort dates
            secure_log("Converting log date / sorting by log_date")
            combined_csv['log_date'] = pd.to_datetime(combined_csv['log_date'],format = "%m/%d/%Y %H:%M:%S")
            combined_csv = combined_csv.sort_values('log_date').reset_index(drop=True)

            initial_length = len(combined_csv)
            #drop duplicates
            combined_csv.drop_duplicates(subset=["account_id", "account_lead_id", "log_subtype_id", "log_id"], keep = "first", inplace = True)
            secure_log("Removed {} duplicates".format(initial_length - len(combined_csv)))

            #start_date = datetime.datetime.strptime(combined_csv['log_date'].iloc[0], "%m/%d/%Y %H:%M:%S %p").strftime("%Y-%m-%d_%H:%M:%S")
            #end_date = datetime.datetime.strptime(combined_csv['log_date'].iloc[len(combined_csv)-1], "%m/%d/%Y %H:%M:%S %p").strftime("%Y-%m-%d_%H:%M:%S")
            start_date = combined_csv['log_date'].iloc[0].strftime("%Y-%m-%d_%H:%M:%S")
            end_date = combined_csv['log_date'].iloc[len(combined_csv)-1].strftime("%Y-%m-%d_%H:%M:%S")

            file_name  = "{}_{}_to_{}.csv".format(customer,start_date,end_date)

            location = "{0}/{1}/{2}".format(csv_directory, customer, file_name)

            # fix NaNs
            secure_log("fixing NaNs ...")
            df = combined_csv

            for col in df:
                try: 
                    df[col] += 0
                    df[col].fillna(0, inplace=True)
                except:
                    df[col].fillna("", inplace=True)

            # convert pandas df to spark df 
            secure_log("Converting pandas to spark dataframe ...")
            file_name = file_name.replace("_","-")
            secure_log("Removing old folders ...")
            remove_old_folder("{}/{}".format(table_name, customer), file_name)

            spark_df = spark.createDataFrame(df)
            s3_path = 's3://{}/{}/{}/{}'.format(bucket,table_name,customer, file_name) #,file_name)
            secure_log("Uploading to S3 ...")
            secure_log(s3_path)
            spark_df.write.save(s3_path, format='csv', index=False, mode='overwrite')

            secure_log("Successfully uploaded merged csv file to S3 for {}".format(customer))
            secure_log("Location: {}/{}/{}/{}".format(bucket, table_name, customer,file_name))

    except Exception as e:
        secure_log("ERROR! {}".format(e))

if __name__ == "__main__":
    main()
