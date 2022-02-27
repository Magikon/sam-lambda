import boto3
import os
import re
import glob
import pandas as pd
import datetime
import psycopg2
import s3fs
from boto3.s3.transfer import TransferConfig

s3_fs = s3fs.S3FileSystem(anon=False)
s3 = boto3.client('s3')

from pplibs.logUtils import secure_log
#from logUtils import secure_log

csv_directory = './csv'
extension = 'csv'

db_name = os.environ["DB_NAME"]
db_user = os.environ["DB_USER"]
env = os.environ["ENV"]

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
    Name="{}-database-password".format(os.environ["ENV"]), 
    WithDecryption=True
) 

db_pass = response['Parameter']['Value']
db_host = os.environ["DB_ENDPOINT"]

def main():
    secure_log('::: Downloading CSVS :::')
    if os.path.isdir("{}".format(csv_directory)) != True:
        os.mkdir("{}".format(csv_directory))

    try:
        all_customers = s3.list_objects(Bucket = os.environ["BUCKET"], Prefix='events-tmp/', Delimiter='/') 
        for o in all_customers['CommonPrefixes']:
            customer = o['Prefix'].split("/")[-2]

            #cleaning directory
            initial_filenames = [i for i in glob.glob('{}/{}/*.{}'.format(csv_directory,customer,extension))]
            for f in initial_filenames:
                os.remove(f)

            if os.path.isdir("{}/{}".format(csv_directory, customer)) != True:
                os.mkdir("{}/{}".format(csv_directory, customer))

            all_customer_events = s3.list_objects(Bucket = os.environ["BUCKET"], Prefix = o['Prefix'])
            for p in all_customer_events['Contents']:
                if(p['Key'] != o['Prefix']):
                    file = p['Key'].split("/")[-1] 
                    if os.path.isfile("{0}/{1}/{2}".format(csv_directory, customer, file)) != True:
                        local_file = "{0}/{1}/{2}".format(csv_directory, customer, file)
                        s3.download_file(os.environ["BUCKET"], p['Key'], local_file)
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
            sq = "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.COLUMNS WHERE TABLE_NAME = 'external_events'"

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

            # upload directly to s3
            with s3_fs.open('s3://{}/external_events/{}/{}'.format(os.environ["BUCKET"],customer,file_name),'w') as f:
                combined_csv.to_csv(f,index=False, encoding='utf-8-sig') #, compression='gzip')

            #export to csv
            # combined_csv.to_csv(location, index=False, encoding='utf-8-sig')
            # secure_log("Successfully merged csv files for {}".format(customer))
            # secure_log("Location: {}".format(location))

            #upload to s3
            # s3u = boto3.resource('s3')
            # s3u.meta.client.upload_file(location, os.environ["BUCKET"], 'external_events/{}/{}'.format(customer,file_name))

            secure_log("Successfully uploaded merged csv dataframe to S3 for {}".format(customer))
            secure_log("Location: {}/external_events/{}/{}".format(os.environ["BUCKET"], customer,file_name))

    except Exception as e:
        secure_log("ERROR! {}".format(e))

if __name__ == "__main__":
    main()
