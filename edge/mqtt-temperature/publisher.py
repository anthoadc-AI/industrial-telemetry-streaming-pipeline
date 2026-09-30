import os
import sys
import time
import random
import paho.mqtt.client as mqtt
import json
PORT = 9883
THINGSBOARD_HOST = 'localhost'

# Device access token, from the environment (see .env.example).
ACCESS_TOKEN = os.environ.get('TB_ACCESS_TOKEN')
if not ACCESS_TOKEN:
    sys.exit('TB_ACCESS_TOKEN is not set; see .env.example')

# Data capture and upload interval in seconds.

sensor_data = {'temperature': 0, 'humidity': 0}

client = mqtt.Client()
# Set access token
client.username_pw_set(ACCESS_TOKEN)

# Connect to ThingsBoard using default MQTT port and 60 seconds keepalive interval
client.connect(THINGSBOARD_HOST, PORT, 60)
client.loop_start()

try:
    while True:
        temperature = random.randint(0, 100)
        humidity = random.randint(50, 100)
        print(f"Temperature: {temperature} humidity: {humidity}")
        sensor_data['temperature'] = temperature
        sensor_data['humidity'] = humidity

        # Sending humidity and temperature data to ThingsBoard
        client.publish('v1/devices/me/telemetry', json.dumps(sensor_data), 1)
        time.sleep(3)
except KeyboardInterrupt:
    pass

client.loop_stop()
client.disconnect()
