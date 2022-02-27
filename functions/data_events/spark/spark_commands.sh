# cd spark
# edit everything you need in spark_dependencies.sh to include any lib you need
# sh spark_dependencies.sh # this will create packages.zip

# export aws_access_key_id=<your-access-key-here>
# export aws_secret_key=<your-secret-key-here>

# cluster creation/provisioning -- not transient 
cluster_name="dev-temp"
release_label="emr-5.27.0"
key_name="propair-etl"
log_uri="s3://aws-logs-450889472107-us-west-1/elasticmapreduce/"
spark_app_name="Spark"
bootstrap_path="s3://spark-etls/bootstrap.sh"
instance_type="m5.xlarge"

aws emr create-cluster --name $cluster_name --release-label $release_label \
--use-default-roles --ec2-attributes KeyName=$key_name \
--log-uri $log_uri \
--enable-debugging \
--applications Name=Spark Name=$spark_app_name \
--instance-count 3 --instance-type $instance_type \
--bootstrap-actions Path=$bootstrap_path

# obtain cluster ID from previous command
cluster_id="j-1ZE9CULSCCZ7N"

# copy source files in master 
aws emr put --cluster-id $cluster_id \
            --key-pair-file "~/.ssh/propair-etl.pem" \
            --src "./spark/merge_csvs_with_spark.py"

aws emr put --cluster-id $cluster_id \
            --key-pair-file "~/.ssh/propair-etl.pem" \
            --src "./spark/packages.zip"

aws emr put --cluster-id $cluster_id \
            --key-pair-file "~/.ssh/propair-etl.pem" \
            --src "./spark/config.yaml"

# aws emr put --cluster-id $cluster_id \
#             --key-pair-file "~/.ssh/propair-etl.pem" \
#             --src "./spark/config.yaml"

# ssh into cluster's master 
cluster_id="j-RJFVP65GROLW"
aws emr ssh --cluster-id $cluster_id \
            --key-pair-file "~/.ssh/propair-etl.pem"

# execute the spark program from the master
export aws_access_key_id=""
export aws_secret_key=""

# client mode
aws emr ssh --cluster-id $cluster_id \
            --key-pair-file "~/.ssh/propair-etl.pem" \
            --command "export PYSPARK_PYTHON=python3 && 
                       export PYSPARK_DRIVER_PYTHON="""/usr/bin/python3""" && 
                       export AWS_ACCESS_KEY_ID=$aws_access_key_id &&
                       export AWS_SECRET_KEY=$aws_secret_key && 
                       spark-submit --py-files packages.zip --files config.yaml merge_csvs_with_spark.py >>log.log"

# cluster mode 
aws emr ssh --cluster-id $cluster_id \
            --key-pair-file "~/.ssh/propair-etl.pem" \
            --command "export PYSPARK_PYTHON=python3 && 
                       export PYSPARK_DRIVER_PYTHON="""/usr/bin/python3""" && 
                       export AWS_ACCESS_KEY_ID=$aws_access_key_id &&
                       export AWS_SECRET_KEY=$aws_secret_key && 
                       spark-submit --master yarn --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON="""/usr/bin/python3"""  --conf spark.yarn.appMasterEnv.AWS_ACCESS_KEY_ID=$aws_access_key_id --conf spark.yarn.appMasterEnv.AWS_SECRET_KEY=$aws_secret_key --deploy-mode cluster --py-files packages.zip --files config.yaml merge_csvs_with_spark.py >>log.log"

# add this to pass in environment vars -- applies to cluster deploy-mode only
# --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON