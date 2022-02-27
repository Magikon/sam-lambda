from events_to_csv_REDSHIFT import update
import json

with open('event.json') as f:
	event = json.load(f)

print(event)
update(event, {})
