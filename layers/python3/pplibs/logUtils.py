
import os
import re
import boto3
import base64
import pydash as _
import xmltodict
import json
from Crypto.Cipher import AES
from Crypto import Random
from Crypto.Hash import SHA256

#globals
account_config = None
accounts_configurations = None
global_lead_id = None
encryption_password = None

dynamodb = boto3.resource('dynamodb')
client = boto3.client('ssm')

def get_account_config(id, refresh = False, cache = True):
    global accounts_configurations
    global account_config

    ### if account configuration does not exist or a refresh as been invoked. 
    ### retrieve the latest configurations for all accounts from dynamoDB.
    ### and return the account configuration specified by id

    if ((accounts_configurations == None) or (refresh == True) or (cache == False)):
        print("::::: Getting Account Configuration")
        config_table = dynamodb.Table('customer-configuration-{}'.format(os.environ['ENV']))
        config_response = config_table.scan()

        accounts_configurations = _.key_by(config_response['Items'], 'id')

        if refresh == False:
            if id in accounts_configurations:
                account_config = accounts_configurations[id]
                return accounts_configurations[id]
            else:
                raise Exception("::::: Configuration for accound id {} not found".format(id))
    else:
        print("::::: Returning Prexisting Account Configuration")
        if id in accounts_configurations:
            account_config = accounts_configurations[id]
            return accounts_configurations[id]
        else:
            raise Exception("::::: Configuration for accound id {} not found".format(id))

def get_encryption_password():
    global encryption_password
    if (encryption_password != None):
      return encryption_password
    else:
      try:
          response = client.get_parameter(
              Name= "/{}/encryption-password".format(os.environ["ENV"]),
              WithDecryption= True
          )
          encryption_password = response['Parameter']['Value']
          return encryption_password
      except Exception as e:
          secure_log("Error retrieving Encryption Password: " + repr(e))
  
  
def secure_log(log, data=None, account_lead_id=None, configuration=None):
    global global_lead_id
    global account_config
    
    try:

        def flatten(d, parent_key='', sep='.'):
            items = []
            for k, v in d.items():
                new_key = parent_key + sep + k if parent_key else k

                if isinstance(v, str):
                    try:
                        test_json = json.loads(v)
                        if isinstance(test_json, dict):
                            v = test_json 
                    except Exception as err:
                        try:
                            text_xml = xmltodict.parse(v)
                            if isinstance(text_xml, dict):
                                raise Exception("Cannot encrypt xml strings")
                        except Exception as e:
                            if (e.args[0] == 'Cannot encrypt xml strings'):
                                raise Exception(e.args[0])
                            pass

                if isinstance(v, dict) and bool(v):
                    items.extend(flatten(v, new_key, sep=sep).items())
                elif isinstance(v, list): 
                    for i, value in enumerate(v):
                        if isinstance(value, str):
                            try:
                                test_json = json.loads(value)
                                if isinstance(test_json, dict):
                                    value = test_json 
                            except Exception as err:
                                try:
                                    text_xml = xmltodict.parse(value)
                                    if isinstance(text_xml, dict):
                                        raise Exception("Cannot encrypt xml strings")
                                except Exception as e:
                                    if (e.args[0] == 'Cannot encrypt xml strings'):
                                        raise Exception(e.args[0])
                                    pass

                        if isinstance(value, dict) and bool(value):
                            items.extend(flatten(value, "{}[{}]".format(new_key,i), sep=sep).items())
                        else:
                            items.append(("{}[{}]".format(new_key,i), value))
                    if (len(v) == 0):
                        items.append((new_key,[]))
                else:
                    items.append((new_key, v))
            return dict(items)

        def mk_int(s):
            try:
                result = int(s)
                return result
            except Exception as e:
                return -1

        def unflatten(data):
            items = {}
            if not isinstance(data, dict) or isinstance(data, list):
                return data

            reg = re.compile("\.?([^.\[\]]+)|\[(\d+)\]")
            for k, v in data.items():
                matches = reg.findall(k)
                cur = items
                prop = ""

                for key in matches:

                    if isinstance(cur, list):
                        for _ in range(prop-len(cur)+1):
                            cur.append(None)

                    if prop in cur or (0 <= mk_int(prop) < len(cur) and cur[prop] != None):
                        cur = cur[prop]
                    elif key[1] != '':
                        cur[prop] = []
                        cur = cur[prop]
                    else:
                        cur[prop] = {}
                        cur = cur[prop]

                    if key[1] != '':
                        prop = int(key[1])
                    else:
                        prop = key[0]

                if isinstance(cur, list):
                    for _ in range(prop-len(cur)+1):
                        cur.append(None)
                cur[prop] = v

            return dict(items)


        PAD = "X"

        def encrypt(text, key, iv):
            while len(text) % 16 != 0:
                text += PAD
            cipher = AES.new(key_hash(key), AES.MODE_CFB, iv)
            encrypted = cipher.encrypt(text.encode())
            return base64.b64encode(encrypted).decode()

        def decrypt(text, key, iv):
            cipher = AES.new(key_hash(key), AES.MODE_CFB, iv)
            plain = cipher.decrypt(base64.b64decode(text))
            return plain.decode().rstrip(PAD)

        def key_hash(key):
            return SHA256.new(key.encode()).digest()

        #This will be our defining object to be encrypted
        obj = None
        
        #1. Retrieve account configuration via arg or global variable
        account_config = configuration if configuration != None else account_config
        config = account_config
        
        #2. Set account name from account config (This value will be appended to every log if available)
        account_name = config['name'] if config != None else None

        #3. Set lead id via arg or global variable (This value will be appended to every log if available)
        global_lead_id = account_lead_id if account_lead_id != None else global_lead_id
        lead_id = global_lead_id if global_lead_id != None else None


        #4. If data is a dict, assign to obj variable as a new dict ( This will create a new reference and prevent mutations to the original object)
        # . Else, assign raw value.
        if (data is not None and isinstance(data, dict)):
            obj = dict(data)
        elif (data is not None):
            obj = data
        
        #5. If only one argument is passed and the value is a dict
        # . Assign it to obj as a new dict ( This is in case the developer passes the object to be encrypted as the only argument )
        if (isinstance(log, dict) and not isinstance(log, Exception) and obj == None):
            obj = dict(log)
            log = ""

        if (isinstance(log, str) or isinstance(log, bytes)):
            try:
                test_json = json.loads(log)
                if isinstance(test_json, dict) and obj == None:
                    log = ''
                    obj = dict(test_json) 
                elif isinstance(test_json, dict):
                    log = 'Unable to encrypt message'
            except Exception as err:
                try:
                    text_xml = xmltodict.parse(log)
                    if isinstance(text_xml, dict):
                        raise Exception("Cannot encrypt xml strings")
                except Exception as e:
                    if (e.args[0] == 'Cannot encrypt xml strings'):
                        raise Exception(e.args[0])
                    pass

        if (isinstance(obj, str) or isinstance(obj, bytes)):
            try:
                test_json = json.loads(obj)
                if isinstance(test_json, dict):
                    obj = dict(test_json) 
            except Exception as err:
                try:
                    text_xml = xmltodict.parse(obj)
                    if isinstance(text_xml, dict):
                        raise Exception("Cannot encrypt xml strings")
                except Exception as e:
                    if (e.args[0] == 'Cannot encrypt xml strings'):
                        raise Exception(e.args[0])
                    pass

        if (obj is not None):

            if (isinstance(obj, dict) and config is not None and 'pii_fields' in config):
                #6. Retrieve encyprtion code and IV
                code = get_encryption_password()
                iv = Random.new().read(AES.block_size)

                #7. Flatten object in order to find PII fields
                tempObj = flatten(obj)

                #8. For Each PII field stated in the account configuration
                # . Search for it in the flattened object and encrypt the values
                for field in config['pii_fields']:
                    for key, value in tempObj.items():
                        PII = re.sub("[^a-zA-Z0-9]", "", str(field)).lower()
                        split = key.split(".")
                        last_key = split[len(split) - 1]

                        final_key = re.sub("[^a-zA-Z0-9]", "", str(last_key)).lower()

                        if (PII == final_key):
                            tempObj[key] = encrypt(value, code, iv)

                #9. Finally, unflatten the object to return it to its original state
                obj=unflatten(tempObj)

            print("[{}{}] {}{}".format(account_name, ", {}".format(lead_id) if lead_id != None else "", log, obj))
        else:
            print("[{}{}] {}".format(account_name, ", {}".format(lead_id) if lead_id != None else "", log))
   
    except Exception as err:
        print(":::::: Secure Log Exception! {}".format(err))