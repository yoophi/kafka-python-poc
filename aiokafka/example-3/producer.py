import asyncio

from aiokafka import AIOKafkaProducer
from loguru import logger

loop = asyncio.get_event_loop()


async def produce_rand_int():
    producer = AIOKafkaProducer(
        loop=loop, bootstrap_servers='localhost:9092')

    # Get cluster layout and initial topic/partition leadership information
    await producer.start()
    try:
        batch = producer.create_batch()

        # Populate the batch. The append() method will return metadata for the
        # added message or None if batch is full.
        for i in range(2):
            metadata = batch.append(value=b"msg %04d" % i, key=None, timestamp=None)
            assert metadata is not None

        # Optionally close the batch to further submission. If left open, the batch
        # may be appended to by producer.send().
        batch.close()

        # Add the batch to the first partition's submission queue. If this method
        # times out, we can say for sure that batch will never be sent.
        fut = await producer.send_batch(batch, "my_topic", partition=1)

        # Batch will either be delivered or an unrecoverable error will occur.
        # Cancelling this future will not cancel the send.
        record = await fut
        logger.info(record)

    finally:
        # Wait for all pending messages to be delivered or expire.
        await producer.stop()


loop.run_until_complete(produce_rand_int())
