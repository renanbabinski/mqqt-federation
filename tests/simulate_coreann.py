import paho.mqtt.client as mqtt
import pickle
import logging

import os
import sys

# Get the absolute path of the 'src' folder
src_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'src'))

# Add the 'src' folder to sys.path
sys.path.append(src_path)

from message import CoreAnn

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# The callback for when the client receives a CONNACK response from the server.
def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))


if __name__ == '__main__':

    client = mqtt.Client(client_id="CoreAnn Simulator")
    client.on_connect = on_connect

    client.connect("127.0.0.1", 1883, 60)  # Connect to the broker



# FIRST CoreAnn
    core_ann = CoreAnn(
        core_id=1,
        dist=1, 
        sender_id=4
    )  # create a CoreAnn instance
    
    payload = pickle.dumps(core_ann)  # serialize the CoreAnn instance

    client.publish("federator/core_ann/topic1", payload)  # publish the serialized CoreAnn


    test = input("Press ENTER to send another CoreAnn...")


## SECOND CoreAnn
    core_ann = CoreAnn(
        core_id=1,
        dist=1, 
        sender_id=7
    )  # create a CoreAnn instance
    
    payload = pickle.dumps(core_ann)  # serialize the CoreAnn instance

    client.publish("federator/core_ann/topic1", payload)  # publish the serialized CoreAnn


    test = input("Press ENTER to send another CoreAnn...")


## THIRD CoreAnn
    core_ann = CoreAnn(
        core_id=1,
        dist=1, 
        sender_id=1
    )  # create a CoreAnn instance
    
    payload = pickle.dumps(core_ann)  # serialize the CoreAnn instance

    client.publish("federator/core_ann/topic1", payload)  # publish the serialized CoreAnn