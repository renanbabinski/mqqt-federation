from message import CoreAnn
import logging

NEIGHBORS_QOS = 2

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class Announcer:
    def __init__(self, federated_topic) -> None:
        self.federated_topic = federated_topic

    def announce(self, ctx) -> None:
        from federator import Context
        ctx: Context = ctx
        ann = CoreAnn(
            core_id=ctx.id,
            dist=0,
            sender_id=ctx.id
        )

        topic, payload = ann.serialize(fed_topic=self.federated_topic)

        for id, neighbor in ctx.neighbors.items():
            neighbor.publish(topic, payload, NEIGHBORS_QOS)

        logger.info(f"WORKER[{self.federated_topic}]: Started announcing as {self.federated_topic} Core...")