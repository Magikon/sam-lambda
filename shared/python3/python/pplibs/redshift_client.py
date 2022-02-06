import base64
import functools
import json

import boto3
import dateutil.parser
import pandas as pd
import psycopg2
from botocore.exceptions import ClientError
from psycopg2.extras import RealDictCursor


class RedshiftClient(object):
    __credentials = None
    __ENV = None

    def __init__(self, env):
        self.connected = False

        if RedshiftClient.__credentials is None or env != RedshiftClient.__ENV:
            RedshiftClient.__ENV = env
            RedshiftClient.__credentials = self.__get_connection_info(env)

    def __repr__(self):
        return f"<Redshift Client. Connected: {self.connected}>"

    def __str__(self):
        return f"<Redshift Client. Connected: {self.connected}>"

    def __get_connection_info(self, db):
        print("::::: Getting Database Credentials")
        secret_name = f"{db}_redshift"
        region_name = "us-west-1"
        secret = None

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            print(e)
            raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = json.loads(get_secret_value_response['SecretString'])
            else:
                secret = json.loads(base64.b64decode(
                    get_secret_value_response['SecretBinary']))

        return secret

    # Connection decorator
    def __connection_required(func):
        @functools.wraps(func)
        def secure_function(self, *args, **kwargs):
            if self.connected:
                return func(self, *args, **kwargs)
            else:
                self.connect()
                result = func(self, *args, **kwargs)
                self.disconnect()
                return result

        return secure_function

    # Required function to enable other client functions
    def connect(self):
        if self.connected is False:
            print("::::: Connecting to database...")
            try:
                self.conn = psycopg2.connect(host=self.__credentials['host'], user=self.__credentials['username'],
                                             password=self.__credentials['password'], database=RedshiftClient.__ENV,
                                             port=self.__credentials['port'])
                self.cur = self.conn.cursor(cursor_factory=RealDictCursor)
                self.connected = True
            except Exception as e:
                raise Exception(f'Unable to connect to database:: {e}')
        else:
            print("::::: Connection already available")

    # close connection to the cluster
    def disconnect(self):
        if self.connected:
            print("::::: Terminating Connection...")
            self.cur.close()
            self.conn.close()
            self.connected = False
        else:
            print("::::: Connection already terminated")

    # submits a query to the cluster
    @__connection_required
    def query(self, sql, return_df=False, name=''):
        print(f"::::: Retrieving Query: {name}")
        self.cur.execute(sql)
        if return_df:
            return pd.DataFrame(self.cur.fetchall())
        else:
            return self.cur.fetchall()

    @__connection_required
    def execute(self, sql, name=''):
        print(f"::::: Executing Query: {name}")
        self.cur.execute(sql)
        self.conn.commit()

    @__connection_required
    def get_column_data(self, table_name, ignored_columns=[]):
        sq = "SELECT \"column\",type from pg_table_def where tablename='{}'".format(table_name)
        count = 0
        successful = False
        data = None

        while not successful and count < 2:
            try:
                self.cur.execute(sq)
                data = self.cur.fetchall()
                if len(data) > 0:
                    successful = True
                else:
                    raise Exception("No column data found")
            except Exception as e:
                print("::::: [{0}] Error - {1} trying again...".format(table_name, e))
                print("::::: [{0}] Num of retries: {1}".format(table_name, count))
            count += 1

        if successful:
            print(f"::::: Fetched column data for {table_name}")
            column_data = {}
            for line in data:
                if line['column'] not in ignored_columns:
                    column_data[line['column']] = {"name": line['column'], "type": line['type']}
            return column_data
        else:
            raise Exception("[{0}] After {1} retries, was unable to retrieve column_data".format(table_name, count))

    @__connection_required
    def get_table_constraints(self, table_name):
        sql = f"select column_name from view_constraints where table_name='{table_name}' and constraint_type='UNIQUE';"
        self.cur.execute(sql)
        out = self.cur.fetchall()
        result = list(set([x['column_name'] for x in out]))
        if len(result) > 0:
            print(f"::::: Fetched table constraints for {table_name}")
            return result
        else:
            raise Exception(f"No table constraints found for {table_name}")

    @__connection_required
    def run_transaction(self, sql, retries=1):
        print(f"::::: Running transaction. Retries: {retries}")
        successful = False
        count = 0
        error = None
        while successful is False and count < retries:
            try:
                self.cur.execute(sql)
                self.con.commit()
                successful = True
            except Exception as err:
                error = err
                print("::::: DB ERROR -> {}. Trying again.. count: {}".format(err, count))
                count += 1

        if successful:
            print(":::: Transaction Complete")
        else:
            print("----> DB error. After {} retries was unable to complete: ".format(retries))
            raise Exception(error)

    ###### Static Query Utilities #######

    @staticmethod
    def build_record_values(record, columns):
        record_values = []

        for x, key in enumerate(columns):
            value = record[key] if key in record else None
            data_type = columns[key]['type']

            if 'timestamp' in data_type and value is not None:
                try:
                    parse_date = dateutil.parser.parse(value)
                    value = parse_date
                except:
                    print('Unable to parse timestamp for column {}'.format(key))
                    value = None
            elif 'date' in data_type and value is not None:
                try:
                    parse_date = dateutil.parser.parse(value)
                    value = parse_date.strftime('%Y-%m-%d')
                except:
                    print('Unable to parse date for column {}'.format(key))
                    value = None

            if value is None or value == '':
                value = "NULL"
            else:
                value = "$${}$$::{}".format(value, data_type)

            record_values.append(value)

        return record_values

    @staticmethod
    def remove_duplicates(records, unique_columns):
        seen = set()
        new_records = []
        for d in records:
            h = {k: d[k] if (d[k] is not None and d[k] != '') else -50 for k in unique_columns if k in d}
            h = tuple(sorted(h.items()))
            if h not in seen:
                seen.add(h)
                new_records.append(d)
        return new_records

    @staticmethod
    def remove_nulls(records, unique_columns):
        new_records = []

        for d in records:

            # Check if all constraints are null
            if all((d[x] is None or d[x] == '') for x in unique_columns if x in d):
                continue
            # Check if all except account_id are null
            if all((d[x] is None or d[x] == '') for x in unique_columns if x in d and x != 'account_id'):
                continue
            new_records.append(d)

        return new_records
