import kafka
import time
import random
import json
from time import sleep
import numpy as np

#define producer and consumer variable
sensor_data = {'longitude': 0, 'latitude': 0}
topic_name = 'vehicle-coordinates'
producer = kafka.KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: 
                         json.dumps(x).encode('utf-8'))

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: {0}: {1}"
              .format(msg.value(), err.str()))
    else:
        print("Message produced: {0}".format(msg.value()))

try:

    while True:
        longitude = np.random.randint(-180, 180)
        latitude = np.random.randint(-90, 90)
        
        print(f"longitude: {longitude} latitude: {latitude}")
        sensor_data['longitude'] = int(longitude)
        sensor_data['latitude'] = int(latitude)
        producer.send(topic_name, value=sensor_data)
        sleep(3)

except KeyboardInterrupt:
    pass