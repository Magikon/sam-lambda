import os
import sys
import pytest
import psycopg2

fileDir = os.path.dirname(os.path.abspath(__file__))
parentDir = os.path.dirname(fileDir)

sys.path.append(parentDir)

from dotenv import load_dotenv
load_dotenv()

from leads_producer import update, get_bin_credit_profile, get_lookup, db_pass, db_host, db_name, db_user, db_port



# General Test variables
conn = psycopg2.connect("dbname={} host={} port={} user={} password={}".format(db_name, db_host, db_port, db_user, db_pass))
cur = conn.cursor()
account_id = 10
credit_profile_lookup = get_lookup(account_id, cur)
account_config = {"credit_profile_priority_list" : ["credit_score_stated", "credit_score_range_stated", "credit_profile_stated"]}

cases = [
    ({'credit_score_stated': 'E&X*C(E)L(L(E(N((((((((((((T'}, None),
    ({'credit_profile_stated': 'FAIR', 'credit_score_stated': '3840', "credit_score_range_stated": "400DR500"}, "Poor"),
    ({'credit_profile_stated': 'FAIR', 'credit_score_stated': '38403', "credit_score_range_stated": "400DR500"}, "Poor"),
    ({'credit_profile_stated': 'FAIR', 'credit_score_stated': '384033', "credit_score_range_stated": "400DR500"}, "Poor"),
    ({'credit_profile_stated': 'FAIR', 'credit_score_stated': '704R733', "credit_score_range_stated": "400DR500"}, "Good"),
    ({'credit_profile_stated': 'FAIR', 'credit_score_stated': '384R$', "credit_score_range_stated": "400DR500"}, "Poor"),
    ({'credit_profile_stated': 'FAIR', 'credit_score_stated': 'EXCELLENT', "credit_score_range_stated": "400DR500"}, "Excellent"),
    ({'credit_profile_stated': 'FAIR', 'credit_score_stated': 'EXCELLENT', "credit_score_range_stated": "800DR810"}, "Excellent"),
    ({'credit_profile_stated': 'FAIR', 'credit_score_stated': 'EXCELLENT', 'source': "HTTPSMYPROPAIRBANK.COMPROPAIRBANKBANKERINDEXR.WEINBERG"}, "Excellent"),
    ({'credit_profile_stated': 'VERYGOOD', 'credit_score_stated': '', "credit_score_range_stated": "XXXXSS@", 'source': "HTTPSMYPROPAIRBANK.COMAPPLYFHAMIP"}, "Excellent"),
    ({'credit_profile_stated': 'PYTESTLOOKUP', 'credit_score_stated': 'VERYGOOD'}, "Excellent"),
    ({'credit_profile_stated': 'VERYGOODSSSSSSSSSS', 'credit_score_stated': '', 'source': "HTTPSMYPROPAIRBANK.COMAPPLYFHAMIP"}, None),
]

@pytest.mark.parametrize("lead, expected", cases )
def test_calculate_credit_profile(lead, expected):
    result = get_bin_credit_profile(lead, account_config, credit_profile_lookup)
    assert result == expected




def test_producer():
    event = {
    "Records": [
        {
        "EventVersion": "1.0",
        "EventSubscriptionArn": "arn:aws:sns:us-west-1:450889472107:pro-pair-serverless-DataArchitectureLeadsTopic-F52C0JVUM93C:a757cfd4-00ce-43d2-b86c-4dfa0f9f0bb2",
        "EventSource": "aws:sns",
        "Sns": {
            "SignatureVersion": "1",
            "Timestamp": "2018-12-13T22:13:08.735Z",
            "Signature": "FyE5Ufyo4LTZxHyNYytWd8D29jNdkHTlRg2V1IE2GUgbfKZhgwyPzRSFmXWL8BGB++JEjx28j/gyuNPGFN6D1AlywW+icHLRrfQ7wEqMx7mZy9ScVeUKrUUlgFuAP3uC3Yazm9U4J1DFL8s7Evx5nJCKfW6W8zxJSAiEHOf0u4FgCa27Q06/BiTb5Si3ndm5650daAJy5ilMo2JxbWWQLvfwoSqjHSPZ/lfUgs/uF8Z3iyEcQDwbeiaq9sbdDIpI6GAm6BOkIYCkjex8iBAmm76s654LqQ/+f6iy4JRZ6+/7m/feULX1IKwUCmU2h4+MhAep79dtxp/sXdPptBI6Yw==",
            "SigningCertUrl": "https://sns.us-west-1.amazonaws.com/SimpleNotificationService-ac565b8b1a6c5d002d285f9598aa1d9b.pem",
            "MessageId": "1604d4c9-e57f-57d9-b590-80453ecb3a17",
            "Message": "{\"velocify_password\": \"df*ejsh#w9kjnDsQm!!akqi\",\"account\": \"propair-bank\",\"account_id\": 10,\"velocify_username\": \"velocifysandbox@pro-pair.com\"}",
            "MessageAttributes": {},
            "Type": "Notification",
            "UnsubscribeUrl": "https://sns.us-west-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-west-1:450889472107:pro-pair-serverless-DataArchitectureLeadsTopic-F52C0JVUM93C:a757cfd4-00ce-43d2-b86c-4dfa0f9f0bb2",
            "TopicArn": "arn:aws:sns:us-west-1:450889472107:pro-pair-serverless-DataArchitectureLeadsTopic-F52C0JVUM93C",
            "Subject": "Production Error in Recommend Lambda"
        }
        }
    ]
    }
    assert update(event,{}) == None

