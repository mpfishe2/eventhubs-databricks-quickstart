import uuid
import datetime
import random
import json
import time
from azure.servicebus import ServiceBusService

sbs = ServiceBusService(service_namespace='<event hubs namespace>', shared_access_key_name='<key name>', shared_access_key_value='<key value>')
devices = []
for x in range(0, 10):
    devices.append(str(uuid.uuid4()))
cities = ['Chicago', 'New York City', 'Los Angeles', 'London', 'Salt Lake City', 'Dallas']
for y in range(0,2000):
    for dev in devices:
        reading = {'id': dev, 'timestamp': str(datetime.datetime.utcnow()), 'uv': random.random(), 'temperature': random.randint(70, 100), 'humidity': random.randint(70, 100), 'city': cities[random.randint(0, 4)]}
        s = json.dumps(reading)
        sbs.send_event('samplehub', s)
    print(y)
    print(reading)
    time.sleep(5)