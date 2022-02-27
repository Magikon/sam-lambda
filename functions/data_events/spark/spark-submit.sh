#! /bin/zsh -
zmodload zsh/zutil
zparseopts -A ARGUMENTS -help: -cluster-id: -key-pair-file: -package-dependencies: -copy-files: -deploy-mode:
declare -a arr=("cluster-id" "key-pair-file" "help" "copy-files" "package-dependencies" "deploy-mode")
declare -a files=("./merge_csvs_with_spark.py" "./packages.zip" "./config.yaml" "./spark-defaults.conf" )
 
#aws emr describe-cluster --cluster-id j-32CL5JIAG042N | jq .Cluster.InstanceGroups[0].InstanceType
#factor=`echo $itype | egrep -o "m5.([0-9]{1,2})" | egrep -o "[^m5.]"`

# default param values
deploy_mode="client"
copy_files="yes"
package_dependencies="no"
help=""
cluster_id=""
key_pair_file="~/.ssh/dkj-propair.pem"
# aws_access_key_id=`echo ${AWS_ACCESS_KEY_ID}`
# aws_secret_access_key=`echo ${AWS_SECRET_ACCESS_KEY}`

help=$ARGUMENTS[--help]

if [[ -n "$help" ]] ; then
    printf '\n'
    printf 'Usage:\n  --help  yes|no\n  --cluster-id\n  --key-pair-file\n  --copy-files  yes|no\n  --package-dependencies  yes|no\n  --deploy-mode  client|cluster'
    printf '\n\n'
    printf '::: Default parameters values :::\n'
    printf '    cluster-id="%s"\n' "$cluster_id"
    printf '    key-pair-file="%s"\n' "$key_pair_file"
    printf '    package-dependencies="%s"\n' "$package_dependencies"
    printf '    copy-files="%s"\n' "$copy_files"
    printf '    deploy-mode="%s"\n' "$deploy_mode"
    printf '\n'
    printf 'NOTE#1: Remember to set your AWS_ACCESS_KEY_ID and AWS_SECRET_KEY\n         ie.: export AWS_ACCESS_KEY_ID=«your-key-here»\n'
    printf 'NOTE#2: You may want to keep a new «tab» or «window» open in your terminal console\n        so you can ssh into your master and tail -f your log.log file\n         ie. once you ssh into your master, type the following command: \n             tail -f log.log'
    printf '\n'
    exit 0
fi

## now loop through the above array
printf '\n'
printf 'Started \n'
for i in "${arr[@]}"
do
    # printf 'i is "%s" \n' "$i"
    arg=$ARGUMENTS[--$i]
    if [[ -n "$arg" ]]; then 
        v=${i/-/_}
        eval $v="$arg"
        # printf 'Argument "%s" has value of "%s" \n' "$i" "$arg"
    fi 
done


if [ "$cluster_id" = "" ]; then 
	printf "Need a cluster id\n  --help yes for assistance\n"
	exit 0
fi

# instance_type=$(aws emr describe-cluster --cluster-id $cluster_id | jq .Cluster.InstanceGroups)
# # | jq .Cluster.InstanceGroups[0].InstanceType)
# print "Instance Type is $instance_type ..."
# #instance_type="m5.3xlarge"
# factor=`echo $instance_type | egrep -o "m5.([0-9]{1,2})" | egrep -o "[^m[0-9].]"`

# if [ "$factor" = "" ]; then 
# 	printf "Default to $driver_memory memory size\n"
# else
# 	driver_memory=`echo "$(($factor * 8))"`
# 	printf "Applying $driver_memory size\n"	
# fi
# exit 0


if [ "$key_pair_file" = "" ]; then 
	printf "Need a key pair file path\n  --help yes for assistance\n"
	exit 0
fi

if [ "$package_dependencies" = "yes" ]; then 	
	printf 'Packaging dependencies ...\n'
	if sh ./spark_dependencies.sh  ; then 
		printf 'new packages.zip created !\n'
	else
		exit 1
	fi
fi

# copy source files in master 
if [ "$copy_files" = "yes" ]; then 
	printf 'Copying files to the Master...\n'
	for f in "${files[@]}"
	do
		if aws emr put --cluster-id $cluster_id \
		            --key-pair-file $key_pair_file \
		            --src $f ; then 
		   printf 'Copy "%s" completed !\n' "$f"
		fi 
	done
fi

# check env vars
# if [ "$aws_access_key_id" = ""  ]; then 
# 	printf "AWS_ACCESS_KEY_ID environment not set \n   --help for assistance\n"
# 	exit 0
# fi
# if [ "$aws_secret_access_key" = ""  ]; then 
# 	printf "AWS_SECRET_ACCESS_KEY environment not set \n   --help for assistance\n"
# 	exit 0
# fi

# remove dependencies and packages.zip
printf 'Setting up dependencies in the Master ...\n'
if aws emr ssh --cluster-id $cluster_id \
            --key-pair-file $key_pair_file \
            --command "rm -rf log.log &&
            		   touch log.log &&
            		   rm -rf ./dependencies empty.txt &&
            		   unzip ./packages.zip" ; then 
   printf 'Dependencies set !\n'
fi

# client mode
if [ "$deploy_mode" = "client" ]; then  
	printf 'Deploying application in «client-mode» ...\n'
	if aws emr ssh --cluster-id $cluster_id \
	            --key-pair-file $key_pair_file \
	            --command "export PYSPARK_PYTHON=python3 && 
	                       export PYSPARK_DRIVER_PYTHON="""/usr/bin/python3""" && 
	                       spark-submit --driver-memory 16g --conf spark.driver.maxResultSize=10g merge_csvs_with_spark.py >>log.log" ; then 
	        printf 'Application submitted to spark in client mode ! \n'
	fi
fi

# cluster mode
if [ "$deploy_mode" = "cluster" ]; then  
	printf 'Deploying application in «cluster-mode» ...\n'
	if aws emr ssh --cluster-id $cluster_id \
            --key-pair-file $key_pair_file \
            --command "export PYSPARK_PYTHON=python3 && 
                       export PYSPARK_DRIVER_PYTHON="""/usr/bin/python3""" && 
                       spark-submit --master yarn --driver-memory 16g --executor-memory 16g --conf spark.driver.maxResultSize=10g spark.yarn.appMasterEnv.PYSPARK_PYTHON=python3 --conf spark.yarn.appMasterEnv.PYSPARK_DRIVER_PYTHON="""/usr/bin/python3"""   --deploy-mode cluster --py-files packages.zip --files config.yaml merge_csvs_with_spark.py >>log.log"
 ; then 
	        printf 'Application submitted to spark in cluster mode ! \n'
	fi
fi
