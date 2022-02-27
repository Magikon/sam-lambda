from events_producer import update
import json

with open('event.json') as f:
	event = json.load(f)

print(event)
update(event, {})
