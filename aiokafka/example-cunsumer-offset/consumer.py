import asyncio

from aiokafka import AIOKafkaConsumer
from kafka import TopicPartition
from loguru import logger

loop = asyncio.get_event_loop()


async def consume():
    consumer = AIOKafkaConsumer(
        'my_topic',
        loop=loop,
        bootstrap_servers='localhost:9092',
        group_id="my-group")
    # Get cluster layout and join group `my-group`
    await consumer.start()
    try:
        # Consume messages
        msg = await consumer.getone()
        logger.info(msg)
        logger.info(f'msg.offset = {msg.offset}')  # Unique msg autoincrement ID in this topic-partition.
        logger.info(f'msg.value = {msg.value}')

        tp = TopicPartition(msg.topic, msg.partition)

        position = await consumer.position(tp)
        # Position is the next fetched offset
        assert position == msg.offset + 1

        committed = await consumer.committed(tp)
        logger.info(f'committed = {committed}')
        # print(committed)

    finally:
        # Will leave consumer group; perform autocommit if enabled.
        await consumer.stop()


loop.run_until_complete(consume())
