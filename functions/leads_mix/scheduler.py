import json
import os
import sys
import traceback
from decimal import Decimal
from datetime import datetime

import boto3

from pplibs.customErrors import report_to_rollbar, ProPairError
from pplibs.logUtils import get_account_config
from pplibs.logUtils import secure_log
from pplibs.redshift_client import RedshiftClient


def decimal_default(obj):
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError


def update(event, context):
    try:

        sns = boto3.client('sns')
        rsc = RedshiftClient(os.environ['ENV'])

        secure_log("::::: Fetching Account Info")
        sq = "SELECT id as account_id, name as account FROM accounts;"
        data = rsc.query(sq, name='Fetch Accounts')

        for account in data:

            account_config = get_account_config(account['account_id'], cache=False)
            run_mix = False

            try:
                run_mix = account_config['mix']['run_mix']
            except KeyError:
                pass

            if run_mix:
                secure_log("::::: Running Mix Tiers for account {}".format(account['account']))

                try:
                    payload = account_config['mix']['tier_config']
                    payload.update(account)
                    payload['today'] = datetime.now().strftime('%Y-%m-%d')

                except KeyError:
                    raise KeyError("Error! Missing Mix Tier Config")

                response = sns.publish(
                    TopicArn=os.environ["TOPIC_MIX_TIERS"],
                    Subject="Mix Tiers Scheduler",
                    Message=json.dumps(payload, default=decimal_default)
                )
                secure_log(response)
            else:
                secure_log(
                    "::::: Run Mix set to false for account {}. Skipping...".format(account['account']))

    except Exception as err:
        secure_log("::::: ERROR :::::")
        secure_log(err)
        stack = traceback.format_exc()
        secure_log(stack)
        rollbar_error = ProPairError(err, error_type="MixTiersScheduler", exec_info=sys.exc_info(), stack=stack)
        report_to_rollbar(os.environ["TOPIC_ROLLBAR"], rollbar_error)


update({}, {})
