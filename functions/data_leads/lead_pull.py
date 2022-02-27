from leads_producer import update
import json

with open('lead.json') as f:
	leads = json.load(f)

update(leads, {})
