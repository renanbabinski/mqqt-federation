import logging
import time
import copy
import asyncio
import queue
import threading
from dataclasses import dataclass
from typing import List, Dict
import paho.mqtt.client as mqtt
from conf import FederatorConfig, BrokerConfig
from message import deserialize, Message, SubLog, FederatedPub, CoreAnn
from topics import CORE_ANNS, MEMB_ANNS, FEDERATED_TOPICS, ROUTING_TOPICS, SUB_LOGS
from worker import TopicWorkerHandle

# Constants
HOST_QOS = 2
NEIGHBORS_QOS = 2

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

rx_queue = asyncio.Queue()
sync_queue = queue.Queue()

@dataclass
class Context:
    id: int
    redundancy: int
    neighbors: Dict[int, mqtt.Client]
    host_client: mqtt.Client


class Federator:
    def __init__(self, ctx:Context, topic_queues:Dict[str, asyncio.Queue]={}) -> None:
        self.ctx = ctx
        self.topic_queues = topic_queues


    async def run(self):
        await self.subscribe()

        while True:
            logger.debug("Waiting messages...")
            mqtt_msg = await rx_queue.get()
            try:
                federated_topic, msg = deserialize(mqtt_msg)
                if federated_topic is None:
                    continue
            except Exception as e:
                logger.error(e)

            # Creating queues and workers
            if federated_topic in self.topic_queues:
                logger.debug(f"A Queue for topic {federated_topic} alredy exist, pushing message to queue...")
                await self.topic_queues[federated_topic].put(msg)
            elif isinstance(msg, SubLog) or isinstance(msg, CoreAnn):
                logger.debug(f"Creating a new Queue and task to handle {federated_topic} messages...")
                worker = TopicWorkerHandle(federated_topic, copy.copy(self.ctx))
                queue = worker.get_queue()
                self.topic_queues[federated_topic] = queue
                await queue.put(msg)
            else:
                logger.error("Message received not dispatched and no new worker was created!")



    async def subscribe(self) -> None:
        logger.debug("Subscribing host to topics...")
        topics = [
            (CORE_ANNS, HOST_QOS),
            (MEMB_ANNS, HOST_QOS),
            (ROUTING_TOPICS, HOST_QOS),
            (FEDERATED_TOPICS, HOST_QOS),
            (SUB_LOGS, HOST_QOS)
        ]

        self.ctx.host_client.subscribe(topics)
        logger.info(f"{topics} Subscribed on host Broker!")

        for id, n_client in self.ctx.neighbors.items():
            n_client.subscribe(topics)
            logger.info(f"{topics} Subscribed in neighbor Broker {id}")
        


async def bridge_queues():
    logger.info("Task bridge queues started!")
    while True:
        while not sync_queue.empty():
            item = sync_queue.get()
            await rx_queue.put(item)
        await asyncio.sleep(0.01)  # avoid busy-waiting



def create_neighbors_clients(configs: FederatorConfig) -> Dict[int, mqtt.Client]:
    neighbors = {}

    for neigh_conf in configs.neighbors:
        logger.debug(f"Creating client for Neighbor Broker {neigh_conf.id, neigh_conf.ip}")
        try:
            client = mqtt.Client(
                client_id=f"Federator #{configs.host.id} Neighbor client {neigh_conf.id}"
            )
            neighbors[neigh_conf.id] = client
        except:
            logger.error(f"Cannot create client for neighbor Broker {neigh_conf.id}")

    return neighbors


def create_host_client(id: int) -> mqtt.Client:
    client = mqtt.Client(
        client_id=f"Federator #{id} Host client {id}"
    )
    
    return client


def connect_host(client: mqtt.Client, ip: str, port: int) -> None:
    client.connect(host=ip, port=port, keepalive=60)
    logger.info("Connected to host Broker!")


def connect_neighbor(id: int, client: mqtt.Client, ip: str, port: int) -> None:
    client.connect(host=ip, port=port, keepalive=60)
    logger.info(f"Connected to neighbor broker {id}!")
    client.loop_start()


def connect_neighbors(neighbors_clients: Dict[int, mqtt.Client], n_configs: List[BrokerConfig]) -> None:
    for n_config in n_configs:
        connect_neighbor(n_config.id, neighbors_clients[n_config.id], n_config.ip, n_config.port)


# Callback for when a message is received from the server.
def on_message(client, userdata: asyncio.Queue, msg: mqtt.MQTTMessage):
    # logger.debug(f"New message in topic {msg.topic}. Message: {msg.payload.decode('utf-8')}")
    sync_queue.put(msg)


def run(config: FederatorConfig) -> None:
    logger.info("Starting federator...")

    neighbors_clients = create_neighbors_clients(config)

    host_client = create_host_client(config.host.id)

    host_client.on_message = on_message

    connect_host(host_client, config.host.ip, config.host.port)

    host_client.loop_start()

    connect_neighbors(neighbors_clients, config.neighbors)
    
    ctx = Context(
        id=config.host.id,
        redundancy=config.redundancy,
        neighbors=neighbors_clients, #Lock
        host_client=host_client
    )

    federator = Federator(
        ctx=ctx
    )

    loop = asyncio.get_event_loop()

    loop.create_task(bridge_queues())

    asyncio.ensure_future(federator.run())

    # Run the event loop until it's stopped
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    # Close the event loop
    loop.close()





    

