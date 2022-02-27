from faker import Faker
from lxml import etree
import os
import math
import re
import random
import xmltodict
import psycopg2
import requests 
import boto3
import json

from utils import meld_columns
from datetime import datetime, timedelta
from helpers.lead import LeadGenerator
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log

fake = Faker()

#### Getting DB credentials
client = boto3.client('ssm')
response = client.get_parameter(
	Name="/{}/redshift/master-database-password".format(os.environ["ENV"]),
	WithDecryption=True
) 

db_host = os.environ["REDSHIFT_ENDPOINT"]  # PRODUCTION

db_name = os.environ["REDSHIFT_DB_NAME"]
db_user = os.environ["REDSHIFT_DB_USER"]
db_port = os.environ["REDSHIFT_DB_PORT"]
db_pass = response['Parameter']['Value']

conn = psycopg2.connect('host={} dbname={} port={} user={} password={}'.format(db_host,db_name,db_port,db_user,db_pass)) 
cur = conn.cursor()

velocify_add_leads_url = "https://service.leads360.com/ClientService.asmx?op=AddLeads"
velocify_post_url = os.environ['LEADS_ENDPOINT']

def send_active_prospect(leads):
    result = []
    secure_log("::::: Processing {} Active Prospect Leads".format(len(leads)))
    for lead in leads:
        try:
            new_lead = dict(lead['lead'])

            post_result = post(lead['post'])
            
            if post_result['custom_rank_bin'] != 0:
                new_lead['ProPairID'] = post_result['propair_id']
    
                result.append(new_lead)
            else:
                secure_log("::::: Rejecting Active Prospect Lead")
            
        except Exception as e:
            print(":::: Error processing Active Prospect lead - {}".format(e))
    
    return result

def fetch_existing_leads(account, cur):
    size = random.randint(1,3)
    date_delay = datetime.now() - timedelta(days=int(random.randint(2,14)))

    sql = """
        select a.payload, b.max
        from leads a
                left join
            (select lead_id, max(rec_count) as max
            from leads_in
            where lead_id is not null
            group by lead_id) b on a.customer_lead_id = b.lead_id
        where a.payload is not null
        and a.customer_id = {account}
        and a.created_at < $${date}$$
        and b.max < 3
        limit {size};
    """.format(account=account['id'], date=date_delay, size=size)
    
    cur.execute(sql)
    data = cur.fetchall()
    result = [json.loads(x[0]) for x in data]

    return result

def fetch_avail_agents(account, cur):
    size = random.randint(5,10)
    
    sql = """
        select agent_id from agent_profiles
        where account_id={}
        and channel <> 'None'
        limit {};
    """.format(account['id'], size)

    cur.execute(sql)
    data = cur.fetchall()

    secure_log("::::: Retrieved {} Available Agents".format(len(data)))
    result = [{'AgentID': str(x[0]), 'Available': str(random.randint(0,1))} for x in data]

    return result


def post(lead, extra = {}):
    post = {'lead': {
        'payload': lead
    }}

    post.update(extra)

    r = requests.post(velocify_post_url, json.dumps(post), headers={"Authorization": "test"})
   
    if r.status_code == 200:
        return json.loads(r.content)
    else:
        raise Exception(r)

def main(event, _context):
    secure_log(":::: Invoking Generate Sandbox Leads Function")

    secure_log('::::: Connecting to database')
    conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
    secure_log('::::: Successfully connected to database')
    cur = conn.cursor()

    sandbox_config = get_account_config(1, cache=False)


    lead_count = sandbox_config['lead_generation']['size']
    counter = 0

    lead_generator = LeadGenerator(cur)

    secure_log("::::: Generating {} Leads".format(lead_count))

    secure_log("::::: Account Probabilities :::::")
    for a in sandbox_config['lead_generation']['accounts']:
        secure_log("::::: {} {}".format(a, sandbox_config['lead_generation']['accounts'][a]))

    leads = []
    ap_leads = []
    while counter < int(lead_count):
        #get random account
        acc = random.choice(list(lead_generator.accounts.keys()))
        acc_config = sandbox_config['lead_generation']['accounts'][acc] if acc in sandbox_config['lead_generation']['accounts'] else None

        if (acc_config):

            if (random.random() < float(acc_config['generation_prob'])):
                leads.append(lead_generator.generate_leads(acc))
                counter += 1

            use_ap = acc_config['active_prospect'] if 'active_prospect' in acc_config else False
            ap_prob = acc_config['active_prospect_probability'] if 'active_prospect_probability' in acc_config else 0.7
            
            if(use_ap and random.random() < float(ap_prob)):
                ap_lead = lead_generator.generate_leads(acc)
                ap_lead['ActiveProspect'] = True
                ap_lead['DateAdded'] = datetime.now().isoformat()
                ap_post = lead_generator.convert_to_post(ap_lead, acc)

                ap_leads.append({'lead': ap_lead, 'post': ap_post})

        else:
            secure_log("::::: No generation config set for account: {}".format(acc))

    # Process Incontact Leads
    for key, account in lead_generator.accounts.items():
        acc_config = sandbox_config['lead_generation']['accounts'][key] if key in sandbox_config['lead_generation']['accounts'] else None

        if acc_config:
            run_incontact = acc_config['incontact'] if 'incontact' in acc_config else False

            if run_incontact:
                ic_leads = fetch_existing_leads(account, cur)

                availability = fetch_avail_agents(account, cur)
                availability = {'availability': {'payload': availability}}

                secure_log("::::: Posting {} InContact Leads".format(len(ic_leads)))
                for l in ic_leads:
                    l['incontact'] = True
                    l['CustomerId'] = str(account['id'])
                    post(l, availability)
                    secure_log("::::: Successfully posted InContact Lead: {}".format(l['id']))

    # Process Active Prospect Leads
    if (len(ap_leads) > 0):
        ap_leads = send_active_prospect(ap_leads)
        leads = leads + ap_leads


    cur.close()
    conn.close()
    fields = lead_generator.velocify.get_fields()
    credential = {
        "username":  lead_generator.velocify.username,
        "password":  lead_generator.velocify.password
    }

    leads_xml = ""
    for lead in leads:
        root = etree.Element('Fields')

        for field in lead:
            f = re.sub("[^0-9A-Z]", "", field.upper())
            matched_field = next(
                    (x for x in fields if
                    f == re.sub("[^0-9A-Z]", "", x['FieldTitle'].upper())),
                    None
                )
            if (matched_field):
                root.append(etree.Element('Field', FieldId=matched_field['FieldId'], Value=str(lead[field])))
            else:
                secure_log("::::: Could not find field {} in Velocify".format(field))

        leads_xml += "<Lead><Campaign CampaignId=\"{}\"/>{}</Lead>".format(lead["CampaignId"], etree.tostring(root))

    headers = {'Content-Type': 'text/xml; charset=utf-8',
               "Host": "service.leads360.com",
               "Content-Length": "length",
               "SOAPAction": "https://service.leads360.com/AddLeads"}

    body = """<?xml version="1.0" encoding="UTF-8"?>
         <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
            <soap:Body>
                <AddLeads xmlns="https://service.leads360.com">
                   <username>{}</username>
                   <password>{}</password>
                   <leads>
                    <Leads>
                        {}
                    </Leads>
                   </leads>
                </AddLeads>
            </soap:Body>
         </soap:Envelope>""".format(credential["username"], credential["password"], leads_xml)

    # sending get request and saving the response as response object
    r = requests.post(velocify_add_leads_url, data=body, headers=headers)
    secure_log(r)

    return json.dumps(leads)

if __name__ == "__main__":
    main({}, {})

