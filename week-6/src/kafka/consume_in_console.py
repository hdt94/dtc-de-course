#!/usr/bin/env python

"""
Consume messages from single or multiple Kafka topics

Based on: https://developer.confluent.io/get-started/python/#build-consumer
"""

from argparse import ArgumentParser
from datetime import datetime

from confluent_kafka import Consumer, KafkaException, OFFSET_BEGINNING


def consume_in_loop(consumer):
    """
    Poll for new messages from Kafka and print them
    """

    while True:
        # poll single message
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.poll
        # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message
        msg = consumer.poll(timeout=2.0)  # timeout in seconds

        current_time = datetime.now().isoformat()
        if msg is None:
            # Initial message consumption may take up to
            # `session.timeout.ms` for the consumer group to
            # rebalance and start consuming
            print(f"{current_time}: waiting...")
        elif msg.error():
            raise KafkaException(msg.error())
        else:
            topic = msg.topic()

            # key and value are binaries
            if msg.key():
                key = msg.key().decode("utf-8")
            else:
                key = None

            (_, ts) = msg.timestamp()  # ts in milliseconds
            ts = datetime.fromtimestamp(ts / 1000).isoformat()
            value = msg.value().decode("utf-8")

            print(f"{current_time}: message: topic={topic}\tkey={key}\tts={ts}\tvalue={value}")

            # manually commit message as enable.auto.commit=False
            consumer.commit(msg)

            # Alternative: consumer.store_offsets(msg)
            # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Consumer.store_offsets


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-t", "--topics", nargs="+", default=["rides"])
    args = parser.parse_args()
    topics = args.topics

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "enable.auto.commit": False,
        "group.id": "consumer.testing",
    })
    consumer.subscribe(topics)

    try:
        consume_in_loop(consumer)
    except KeyboardInterrupt:
        print("\nCanceled by user")
    finally:
        print("Closing consumer...")

        # Leave group and commit final offsets
        consumer.close()
