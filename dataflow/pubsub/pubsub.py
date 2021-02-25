import os
from datetime import datetime
import time
import json

from google.cloud import pubsub_v1

# gcloud pubsub topics create demo-topic
# gcloud pubsub topics delete demo-topic

PROJECT = os.environ['PROJECT_NAME']
PUBSUB_TOPIC = os.environ['TOPIC']

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT, PUBSUB_TOPIC)

with open('invoices.json') as fp:
    line = fp.readline()
    while line:
        obj = json.loads(line)
        payments = obj['added_payments']
        payments[0]['added_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        obj['added_payments'] = payments
        obj['created_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        line = json.dumps(obj)
        # data = base64.b64decode(line)
        publisher.publish(topic_path, data=line.encode('utf-8'))
        print(line)
        time.sleep(0.01)
        line = fp.readline()
