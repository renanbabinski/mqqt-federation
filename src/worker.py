import logging
import asyncio
from message import SubLog, FederatedPub, CoreAnn, MeshMembAnn, PubId, RoutedPub
import paho.mqtt.client as mqtt
from announcer import Announcer
import copy
from lru import LRUCache

HOST_QOS = 2
NEIGHBORS_QOS = 2

logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


class TopicWorkerHandle:
    def __init__(self, federated_topic: str, ctx) -> asyncio.Queue:
        from federator import Context
        self.ctx: Context = ctx
        self.queue = asyncio.Queue()
        self.worker = TopicWorker(federated_topic, ctx, self.queue)
        asyncio.create_task(self.worker.start())

        logger.info(f"Spawned new worker for topic {federated_topic}")
    
    def get_queue(self):
        return self.queue


class CoreBroker:
    def __init__(self, id, dist, parents: list) -> None:
        self.id = id
        self.dist = dist
        self.parents = parents


# class Parent:
#     def __init__(self, id) -> None:
#         self.id = id

class TopicWorker:
    def __init__(self, topic: str, ctx, queue: asyncio.Queue()) -> None:
        from federator import Context
        self.topic = topic
        self.ctx:Context = ctx
        self.queue = queue
        self.children = []
        self.current_core = None
        self.next_id = 0
        self.cache = LRUCache(self.ctx.cache_size)
        self.has_local_subs = False

    async def start(self):
        while True:
            msg = await self.queue.get()
            logger.debug(f"WORKER[{self.topic}]: Message received {msg}")
            await self.handle(msg)

    async def handle(self, msg):
        if isinstance(msg, SubLog):
            logger.debug(f"WORKER[{self.topic}]:Handle new sub...")
            await self.handle_sub()
        elif isinstance(msg, CoreAnn):
            logger.debug(f"WORKER[{self.topic}]:Handle CoreAnn...")
            await self.handle_core_ann(msg)
        elif isinstance(msg, MeshMembAnn):
            logger.debug(f"WORKER[{self.topic}]:Handle MeshMembAnn...")
            await self.handle_memb_ann(msg)
        elif isinstance(msg, FederatedPub):
            logger.debug(f"WORKER[{self.topic}]:Handle FederatedPub...")
            await self.handle_publication(msg)
        elif isinstance(msg, RoutedPub):
            logger.debug(f"WORKER[{self.topic}]:Handle RoutedPub...")
            await self.handle_routed_pub(msg)
        else:
            logger.error(f"WORKER[{self.topic}]:No Handle for this message type!")


    async def handle_sub(self):
        # Two Ways:
        # - Topic doesn't have a core - Announce Core Broker!
        # - Topic have a core Broker - Member Ann ????
        if self.current_core == None:
            logger.debug(f"WORKER[{self.topic}]: Will start announcing as {self.topic} Core...")
            announcer = Announcer(self.topic)
            announcer.announce(copy.copy(self.ctx))
            self.has_local_subs = True
            self.current_core = self.ctx.id
            self.children.clear() ## Verify if is necessary

        else:  ## Current Core is another broker
            if isinstance(self.current_core, CoreBroker):
                logger.debug(f"WORKER[{self.topic}]: Answer parents...")
                self.has_local_subs = True
                await self.answer_parents()



    async def handle_core_ann(self, core_ann: CoreAnn):
        logger.debug(f"WORKER[{self.topic}]:Handling CoreAnn...")

        if core_ann.core_id == self.ctx.id or core_ann.sender_id == self.ctx.id:
            logger.debug(f"WORKER[{self.topic}]:Core ID or Sender are Myself!")
            return
        
        core_ann.dist += 1 # consider distance from the neighbor to me
        
        # Two ways:
        # - Topic doesn't have a core
        # - Topic have a core broker
        if self.current_core == None:
            logger.info(f"WORKER[{self.topic}]:{core_ann.core_id} is New core elected!")

            self.children.clear()

            parents = []

            parents.append(core_ann.sender_id)

            new_core = CoreBroker(
                id=core_ann.core_id,
                dist=core_ann.dist,
                parents=parents
            )

            self.current_core = new_core

            await self.forward(core_ann)
        else:
            # 2 ways:
            # - Same CoreAnn received - Update parents and childs if necessary
            # - Another CoreAnn for same topic - Tiebreaker

            logger.debug(f"WORKER[{self.topic}]:Received CoreAnn but already has a Core Broker!")

            current_core_id = self.current_core.id if isinstance(self.current_core, CoreBroker) else self.current_core
            logger.debug(f"WORKER[{self.topic}]:CURRENT CORE ID {current_core_id}")

            if core_ann.core_id == current_core_id: # Same CoreAnn received from another source
                core:CoreBroker = self.current_core

                if core.dist == core_ann.dist:
                    logger.debug(f"WORKER[{self.topic}]:Received CoreAnn with same distance, creating new parent!")
                    if core_ann.sender_id not in core.parents:
                        if all(core_ann.sender_id < parent_id for parent_id in core.parents) or len(core.parents) < self.ctx.redundancy:
                            if len(core.parents) == self.ctx.redundancy: 
                                # Remove higher parent id from list
                                logger.debug(f"WORKER[{self.topic}]:Parent list is FULL, removing the higher parent...")
                                max_id = max(core.parents)
                                core.parents.remove(max_id)

                            core.parents.append(core_ann.sender_id)
                    else:
                        logger.warning(f"WORKER[{self.topic}]:CoreAnn Sender already in CoreBroker parents list")
                elif core.dist > core_ann.dist:
                    logger.debug(f"WORKER[{self.topic}]:Received CoreAnn with LESS distance, CLEANING PARENTS!")
                    core.dist = core_ann.dist

                    core.parents.clear()
                    core.parents.append(core_ann.sender_id)

                    await self.forward(core_ann)
                else: 
                    pass # Greater distance: Do nothing


            elif core_ann.core_id < current_core_id: # Tiebreaker - To implement (Or not)
                logger.debug(f"WORKER[{self.topic}]:TIEBREAKER!")
                pass
        
        try:  ## Change this
            logger.debug(f"WORKER[{self.topic}]: Parent list: {self.current_core.parents}") 
        except:
            logger.debug(f"WORKER[{self.topic}]: Parent list: {self.current_core}")




    async def handle_memb_ann(self, memb_ann: MeshMembAnn):
        logger.debug(f"WORKER[{self.topic}]:Handling MeshMembAnn...")
        if memb_ann.sender_id == self.ctx.id:
            logger.debug(f"WORKER[{self.topic}]:Sender are Myself!")
            return
        
        logger.info(f"WORKER[{self.topic}]:Received a Mesh Member Announcement")

        current_core_id = self.current_core.id if isinstance(self.current_core, CoreBroker) else self.current_core

        if current_core_id == memb_ann.core_id:
            if memb_ann.core_id == self.ctx.id:
                if memb_ann.sender_id not in self.children:
                    self.children.append(memb_ann.sender_id)
            else:
                if memb_ann.sender_id not in self.children:
                    self.children.append(memb_ann.sender_id)
                logger.debug(f"WORKER[{self.topic}]: Answer parents...")
                await self.answer_parents()
        else:
            logger.error(f"WORKER[{self.topic}]:current_core_id and MeshMembAnn.core_id does not match!")

        logger.debug(f"WORKER[{self.topic}]: Children list: {self.children}")


    async def handle_publication(self, federated_pub: FederatedPub):
        logger.debug(f"WORKER[{self.topic}]:Handling FederatedPub...")
        new_id = PubId(
            origin_id=self.ctx.id,
            seqn=self.next_id
        )

        logger.debug(f"WORKER[{self.topic}]:current PubId seqn: [{new_id.seqn}]")

        self.next_id += 1

        topic, payload = RoutedPub(
            pub_id=new_id,
            payload=federated_pub.payload,
            sender_id=self.ctx.id
        ).serialize(self.topic)

        # cache the message id to prevent it from being routed twice
        self.cache.put(new_id, None)

        # Send to mesh parents
        if isinstance(self.current_core, CoreBroker):
            logger.debug(f"WORKER[{self.topic}]:Sending RoutedPub to parents...")
            await self.send_to(topic, payload, self.current_core.parents)


        # Send to mesh children
        logger.debug(f"WORKER[{self.topic}]:Sending RoutedPub to children...")
        await self.send_to(topic, payload, self.children)

        logger.debug(f"WORKER[{self.topic}]:Cache[{self.cache}]")



    async def handle_routed_pub(self, routed_pub: RoutedPub):
        logger.debug(f"WORKER[{self.topic}]:Handling RoutedPub...")
        # Check if message was already routed
        if self.cache.contains(routed_pub.pub_id):
            logger.debug(f"WORKER[{self.topic}]:CACHE: Already routed this pub: {routed_pub.pub_id}")
            return
        
        self.cache.put(routed_pub.pub_id, None)

        # Send to local subscribers
        if self.has_local_subs:
            logger.debug(f"WORKER[{self.topic}]:Routing pub to local subs...")

            topic, payload = FederatedPub(
                payload=routed_pub.payload
            ).serialize(self.topic)
            
            self.ctx.host_client.publish(topic, payload, HOST_QOS)
        
        # Set sender_id to myself
        sender_id = routed_pub.sender_id
        routed_pub.sender_id = self.ctx.id

        topic, payload = routed_pub.serialize(self.topic)

        # Send to mesh parents
        if isinstance(self.current_core, CoreBroker):
            logger.debug(f"WORKER[{self.topic}]:Sending RoutedPub to parents...")
            parents = list(self.current_core.parents)
            try:  # Try to remove sender id from parents, if not exist, continue
                parents.remove(sender_id)
            except ValueError:
                pass
            await self.send_to(topic, payload, parents)

        # Send to mesh children
        logger.debug(f"WORKER[{self.topic}]:Sending RoutedPub to children...")
        children = list(self.children)
        try:  # Try to remove sender id from parents, if not exist, continue
            children.remove(sender_id)
        except ValueError:
            pass
        await self.send_to(topic, payload, children)
        


    async def forward(self, core_ann: CoreAnn):
        topic, payload = CoreAnn(
            core_id=core_ann.core_id,
            dist=core_ann.dist,
            sender_id= self.ctx.id
        ).serialize(self.topic)

        for id, neighbor in self.ctx.neighbors.items():
            if id != core_ann.sender_id:
                neighbor.publish(topic, payload, NEIGHBORS_QOS)

    async def answer_parents(self):
        topic, payload = MeshMembAnn(
            core_id=self.current_core.id,
            sender_id=self.ctx.id
        ).serialize(self.topic)

        for id, neighbor in self.ctx.neighbors.items():
            if id in self.current_core.parents:
                neighbor.publish(topic, payload, NEIGHBORS_QOS)


    async def send_to(self, topic:str, payload, ids:list):
        for id in ids:
            logger.debug(f"WORKER[{self.topic}]:Sending RoutedPub to [{id}]")
            neighbor_client = self.ctx.neighbors.get(id, None)
            if neighbor_client is not None:
                neighbor_client.publish(topic, payload, NEIGHBORS_QOS)
            else:
                logger.error(f"WORKER[{self.topic}]:broker {id} is not a neighbor")
