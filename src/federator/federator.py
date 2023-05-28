import logging
import paho.mqtt.client as mqtt

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def on_connect(client:mqtt.Client, userdata, flags, rc):
    print("Connected with result code "+str(rc))

    # Subscribing in on_connect() means that if we lose the connection and
    # reconnect then subscriptions will be renewed.
    
    topics = [
        ('federator/core_ann/#', 0),
        ('federator/memb_ann/#', 0),
        ('federator/routing/#', 0)
    ]

    client.subscribe(topics)
    print(f"Subscribed in topics: {topics}")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print(msg.topic+" "+str(msg.payload))

def on_core_ann(client, userdata, msg):
    print("I handle Core Annoucements!")
    print(msg.topic+" "+str(msg.payload))

def on_memb_ann(client, userdata, msg):
    print("I handle Member Annoucements!")
    print(msg.topic+" "+str(msg.payload))

def on_routing(client, userdata, msg):
    print("I handle Routing messages!")
    print(msg.topic+" "+str(msg.payload))


def run() -> None:
    logger.info("Starting federator...")

    host_client = mqtt.Client()
    host_client.on_connect = on_connect
    host_client.on_message = on_message
    host_client.on_subscribe

    host_client.message_callback_add(sub='federator/core_ann/#', callback=on_core_ann)
    host_client.message_callback_add(sub='federator/memb_ann/#', callback=on_memb_ann)
    host_client.message_callback_add(sub='federator/routing/#', callback=on_routing)


    host_client.connect("localhost", 1881, 60)

    host_client.loop_forever()


class Federator:
    def __init__(self) -> None:
        pass

    

