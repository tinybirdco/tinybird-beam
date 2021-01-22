import base64
import json
import time

from google.cloud import pubsub_v1

# gcloud pubsub topics create demo-topic
# gcloud pubsub topics delete demo-topic

PROJECT = 'stddevco'
PUBSUB_TOPIC = f'demo-topic'

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT, PUBSUB_TOPIC)

with open('invoices.json') as fp:
    line = fp.readline()
    while line:
        # data = base64.b64decode(line)
        publisher.publish(topic_path, data=line.encode('utf-8'))
        print(line)
        time.sleep(0.1)
        line = fp.readline()
