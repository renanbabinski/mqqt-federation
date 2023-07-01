import paho.mqtt.client as mqtt
import pickle
import logging
import toml
import time

import os
import sys

# Get the absolute path of the 'src' folder
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Add the 'src' folder to sys.path
sys.path.append(src_path)

from topics import ROUTING_TOPICS, CORE_ANNS, MEMB_ANNS, FEDERATED_TOPICS, SUB_LOGS
from message import *

EXCLUDED_TOPICS = [ROUTING_TOPICS, CORE_ANNS, MEMB_ANNS]

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class Host:
    def __init__(self, id, ip, port):
        self.id = id
        self.ip = ip
        self.port = port

def on_connect(client, userdata, flags, rc):
    client.subscribe(CORE_ANNS)
    client.subscribe(MEMB_ANNS)
    client.subscribe(FEDERATED_TOPICS)
    client.subscribe(ROUTING_TOPICS)
    client.subscribe(SUB_LOGS)

def on_message(client, userdata, msg):
    if not any(topic in msg.topic for topic in EXCLUDED_TOPICS):
        try:
            print(f'{userdata.id} - {msg.topic} - {msg.payload.decode()}')
        except:
            payload = pickle.loads(msg.payload)
            print(f'{userdata.id} - {msg.topic} - {payload}')


if __name__ == '__main__':
    hosts = []
    data = toml.load('hosts.toml')
    for host in data['host']:
        hosts.append(Host(host['id'], host['ip'], host['port']))

    clients = []

    for host in hosts:
        client = mqtt.Client(userdata=host)
        client.on_connect = on_connect
        client.on_message = on_message
        client.connect(host.ip, host.port, 60)
        clients.append(client)

    # Start the loop
    for client in clients:
        client.loop_start()

    # Keep the script running
    while True:
        time.sleep(1)