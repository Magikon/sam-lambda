#! /bin/zsh -
zmodload zsh/zutil
zparseopts -A ARGUMENTS -create-cluster: -cluster-name: -release-label: -key-name: -log-uri: -spark-app-name: -bootstrap-path: -instance-type: -delete-cluster: -cluster-id: -help: -instance-count:
declare -a arr=("create-cluster" "cluster-name" "release-label" "key-name" "log-uri" "spark-app-name" "bootstrap-path" "instance-type" "delete-cluster" "cluster-id" "help" "instance-count")

# default param values
help=""
create_cluster="no"
delete_cluster="no"
cluster_id=""
cluster_name="dev-temp"
release_label="emr-5.27.0"
key_name="propair-etl"
log_uri="s3://aws-logs-450889472107-us-west-1/elasticmapreduce/"
spark_app_name="Spark"
bootstrap_path="s3://spark-etls/bootstrap.sh"
instance_type="m5.xlarge"
instance_count=3

help=$ARGUMENTS[--help]

if [[ -n "$help" ]] ; then
    printf '\n'
    printf 'Usage:\n  --create-cluster  yes|no \n  --cluster-name\n  --release-label\n  --key-name \n  --log-uri\n  --spark-app-name\n  --bootstrap-path\n  --instance-type\n  --instance-count\n  --delete-cluster  yes|no\n  --cluster-id\n'
    printf '\n'
    printf '::: Default parameters values :::\n'
    printf '    cluster-name="%s"\n' "$cluster_name"
    printf '    release-label="%s"\n' "$release_label"
    printf '    create-cluster="%s"\n' "$create_cluster"
    printf '    key_name="%s"\n' "$key_name"
    printf '    log-uri="%s"\n' "$log_uri"
    printf '    spark-app-name="%s"\n' "$spark_app_name"
    printf '    bootstrap-path="%s"\n' "$bootstrap_path"
    printf '    instance-type="%s"\n' "$instance_type"
    printf '    instance-count="%s"\n' "$instance_count"
    printf '    delete-cluster="%s"\n' "$delete_cluster"
    printf '    cluster-id="%s"\n' "$cluster_id"
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


# crete cluster
if [ "$create_cluster" = "yes" ] ; then 
    printf "Creating cluster ...\n"
    printf "Uploading bootstrap ...\n"
    if aws s3 cp bootstrap.sh s3://spark-etls; then 
        printf "Bootstrap uploaded !"
    fi
    printf "Provisioning AWS EMR Spark Cluster ...\n"
    if aws emr create-cluster \
        --name $cluster_name \
        --release-label $release_label \
        --use-default-roles \
        --ec2-attributes KeyName=$key_name \
        --log-uri $log_uri \
        --enable-debugging \
        --applications Name=Spark Name=$spark_app_name \
        --instance-count $instance_count \
        --instance-type $instance_type \
        --bootstrap-actions Path=$bootstrap_path ; then
            printf "Cluster created !\n"
    fi
fi

# delete cluster 
if [ "$delete_cluster" = "yes" ] ; then 
    printf "Deleting cluster ..."
    if aws emr terminate-clusters --cluster-ids $cluster_id ; then 
        printf 'Cluster "%s" terminated ! \n' "$cluster_id"
    fi
fi
