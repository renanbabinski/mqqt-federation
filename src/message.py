import paho.mqtt.client as mqtt
import fnmatch
import logging
import pickle
from typing import Tuple
from dataclasses import dataclass
from topics import CORE_ANN_TOPIC_LEVEL, MEMB_ANN_TOPIC_LEVEL, FEDERATED_TOPICS_LEVEL, ROUTING_TOPICS_LEVEL, SUB_LOGS_TOPIC_LEVEL
from topics import CORE_ANNS, MEMB_ANNS, ROUTING_TOPICS, SUB_LOGS, FEDERATED_TOPICS


logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# frozen=True make this dataclass immutable and give __hash__ method to class
@dataclass(frozen=True)
class PubId:
    origin_id:int
    seqn:int

class SubLog:
    def __init__(self, payload) -> None:
        self.payload = payload

    def __str__(self) -> str:
        return f"SubLog(payload={self.payload})"

class FederatedPub:
    def __init__(self, payload) -> None:
        self.payload = payload

    def __str__(self) -> str:
        return f"FederatedPub(payload={self.payload})"
    
    def serialize(self, fed_topic: str) -> Tuple[str, bytes]:
        topic = f"{FEDERATED_TOPICS_LEVEL}{fed_topic}"

        return topic, self.payload

class CoreAnn:
    def __init__(self, core_id, dist, sender_id) -> None:
        self.core_id = core_id
        self.dist = dist
        self.sender_id = sender_id

    def __str__(self) -> str:
        return f"CoreAnn(core_id={self.core_id}, dist={self.dist}, sender_id={self.sender_id})"

    def serialize(self, fed_topic: str) -> Tuple[str, bytes]:
        topic = f"{CORE_ANN_TOPIC_LEVEL}{fed_topic}"
        payload = pickle.dumps(self)

        return topic, payload
    
class MeshMembAnn:
    def __init__(self, core_id:int, sender_id: int) -> None:
        self.core_id = core_id
        self.sender_id = sender_id

    def __str__(self) -> str:
        return f"MeshMembAnn(core_id={self.core_id}, sender_id={self.sender_id})"

    def serialize(self, fed_topic: str) -> Tuple[str, bytes]:
        topic = f"{MEMB_ANN_TOPIC_LEVEL}{fed_topic}"
        payload = pickle.dumps(self)

        return topic, payload
    
class RoutedPub:
    def __init__(self, pub_id: PubId, sender_id:int, payload) -> None:
        self.pub_id = pub_id
        self.sender_id = sender_id
        self.payload = payload

    def __str__(self) -> str:
        return f"RoutedPub(pub_id={self.pub_id}, sender_id={self.sender_id}, payload={self.payload})"

    def serialize(self, fed_topic: str) -> Tuple[str, bytes]:
        topic = f"{ROUTING_TOPICS_LEVEL}{fed_topic}"
        payload = pickle.dumps(self)

        return topic, payload

def deserialize(mqtt_msg: mqtt.MQTTMessage) -> Tuple[str, object]:
    topic:str = mqtt_msg.topic
    if topic.startswith(SUB_LOGS_TOPIC_LEVEL):
        fed_topic = mqtt_msg.payload.decode('utf-8').split(' ')[-1] ## Get last element (topic)
        if  fnmatch.fnmatch(fed_topic, SUB_LOGS) or \
            fnmatch.fnmatch(fed_topic, ROUTING_TOPICS) or \
            fnmatch.fnmatch(fed_topic, MEMB_ANNS) or \
            fnmatch.fnmatch(fed_topic, FEDERATED_TOPICS) or \
            fnmatch.fnmatch(fed_topic, CORE_ANNS):
            
            logger.debug("SubLog Received in management topic - Droping Message...")
            return None, None
        else:
            payload = mqtt_msg.payload.decode('utf-8')
            sub_log = SubLog(
                payload
            )
            return fed_topic, sub_log
        
    elif topic.startswith(CORE_ANN_TOPIC_LEVEL):
        fed_topic = topic[len(CORE_ANN_TOPIC_LEVEL):]
        assert fed_topic, "Empty federated topic"
        core_ann = pickle.loads(mqtt_msg.payload)
        return fed_topic, core_ann
    
    elif topic.startswith(MEMB_ANN_TOPIC_LEVEL):
        fed_topic = topic[len(MEMB_ANN_TOPIC_LEVEL):]
        assert fed_topic, "Empty federated topic"
        memb_ann = pickle.loads(mqtt_msg.payload)
        return fed_topic, memb_ann
    
    elif topic.startswith(ROUTING_TOPICS_LEVEL):
        fed_topic = topic[len(ROUTING_TOPICS_LEVEL):]
        assert fed_topic, "Empty federated topic"
        routed_pub = pickle.loads(mqtt_msg.payload)
        return fed_topic, routed_pub
    
    # Federated Publications will ever be last match "#"
    elif topic.startswith(FEDERATED_TOPICS_LEVEL):
        fed_topic = topic[len(FEDERATED_TOPICS_LEVEL):]
        assert fed_topic, "Empty federated topic"
        payload = mqtt_msg.payload
        federated_pub = FederatedPub(
            payload
        )
        return fed_topic, federated_pub

    else:
        raise ValueError(f"Received a packet from a topic it was not supposed to be subscribed to {topic}")
