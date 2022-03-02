import os
import pydash as _
import boto3

from decimal import Decimal

dynamodb = boto3.resource('dynamodb')
kafka_config = None

client = boto3.client('ssm')


def fetch_kafka_pass():
    #### Getting Kafka credentials
    response = client.get_parameter(
        Name="/{}/kafka-password".format(os.environ['ENV']), 
        WithDecryption=True
    ) 
    return response['Parameter']['Value']

def replace_decimals(obj):
    if isinstance(obj, list):
        for i, k in enumerate(obj):
            obj[i] = replace_decimals(obj[i])
        return obj
    elif isinstance(obj, dict):
        for k in obj.keys():
            obj[k] = replace_decimals(obj[k])
        return obj
    elif isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        else:
            return float(obj)
    else:
        return obj

def get_kafka_config(cache=True):
    global kafka_config

    ### if account configuration does not exist or a refresh as been invoked.
    ### retrieve the latest configurations for all accounts from dynamoDB.
    ### and return the account configuration specified by id

    if (kafka_config is None) or (cache is False):
        print(f"::::: Getting Kafka Configuration; Environment: {os.environ['ENV']}")
        config_table = dynamodb.Table('system-configuration')
        config_response = config_table.get_item(
            Key={'name': 'kafka'}
        )

        if 'Item' in config_response:
            kafka_config = config_response['Item'][os.environ['ENV']]
            kafka_config = replace_decimals(kafka_config)
            return kafka_config
        else:
            raise Exception("Could not find kafka configuration in <dynamodb.system-configuration>")

    else:
        print("::::: Returning Prexisting Kafka Configuration")
        return kafka_config

