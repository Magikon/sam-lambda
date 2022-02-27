secure_log('::: Downloading CSVS :::')
if os.path.isdir("{}".format(csv_directory)) != True:
	os.mkdir("{}".format(csv_directory))

prefix = 'events-tmp'	
all_customers = s3.list_objects(Bucket = bucket, Prefix='{}/'.format(prefix), Delimiter='/') 

for o in all_customers['CommonPrefixes']:
	customer = o['Prefix'].split("/")[-2]
	secure_log('Load/Merge All Files From S3')
	all_customer_events = s3.list_objects(Bucket = bucket, Prefix = o['Prefix'])
	Nfile = 1 
	for p in all_customer_events['Contents']:
		if(p['Key'] != o['Prefix']):
			file_name = p['Key'].split("/")[-1] 
			print(file_name)
			if (Nfile == 1):
				sdf = spark.read.csv('s3n://{}/{}/{}/{}'.format(bucket,prefix,customer,file_name),header=True)
			else:
				sdf = sdf.union(spark.read.csv('s3n://{}/{}/{}/{}'.format(bucket,prefix,customer,file_name),header=True))
			Nfile += 1
	secure_log('Dataframe Size . (rows,columns) = ({},{})'.format(sdf.count(), len(sdf.columns)))
	secure_log('Sort by Log Date')
	sdf = sdf.orderBy('log_date')

import dateutil.parser
yourdate = dateutil.parser.parse(log)

date_pattern = "%Y-%m-%d %H:%M:%S"

def get_date(s_date):
	date_patterns = ["%d-%m-%Y", "%Y-%m-%d"]
	for pattern in date_patterns:
		try:
			return datetime.datetime.strptime(s_date, pattern).date()
		except:
			pass
	print("Date is not in expected format: {}".format(s_date) )