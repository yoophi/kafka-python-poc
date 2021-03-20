import asyncio

from aiokafka import AIOKafkaConsumer
from loguru import logger

loop = asyncio.get_event_loop()


async def consume():
    consumer = AIOKafkaConsumer(
        'my_topic',
        'my_other_topic',
        loop=loop,
        bootstrap_servers='localhost:9092',
        group_id="my-group")
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        async for msg in consumer:
            logger.debug(msg)
            print("consumed: ", msg.topic, msg.partition, msg.offset,
                  msg.key, msg.value, msg.timestamp)
    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


loop.run_until_complete(consume())
