import asyncio
import sys

from aiokafka import AIOKafkaConsumer
from loguru import logger

loop = asyncio.get_event_loop()

group_id = 'my-group'

try:
    group_id = sys.argv[1]
except:
    pass

logger.info(f'group_id = {group_id}')


async def consume():
    consumer = AIOKafkaConsumer(
        'my_topic',
        loop=loop,
        bootstrap_servers='localhost:9092',
        group_id=group_id,
        enable_auto_commit=False,
        )

    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        while True:
            async for msg in consumer:
                logger.debug(msg)
                print("consumed: ", msg.topic, msg.partition, msg.offset,
                      msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


loop.run_until_complete(consume())
