#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Consume data from Apache Kafka streams and upload it into Prometheus.

Usage: dispatcher.py
"""
import sys
import json
from time import sleep
from typing import List
from confluent_kafka import Consumer, Message, KafkaError, KafkaException
from prometheus_client import start_http_server, Counter, Gauge, CollectorRegistry, pushadd_to_gateway

# Define Kafka properties
conf = {
    "bootstrap.servers": "kafka:29092",
    "group.id": "data.dispatcher",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

MIN_COMMIT_COUNT = 5

# Create a registry and a gauge metric
registry = CollectorRegistry()
# Define Prometheus metrics
reviews_processed_total = Counter("hotel_reviews_ratings_processed_total", "Total number of messages processed")
review_rating = Gauge("hotel_review_rating", "Rating of the review", ["review"], registry=registry)

def upload_to_prometheus(message: Message) -> None:
    """Transform the message to prometheus format and upload it.

    Args:
        message (Message): hotel review
    """
    key = message.key()
    val = message.value().decode("utf-8")
    timestamp = message.timestamp()
    topic = message.topic()

    val_json = json.loads(val)

    print("******")
    print("Topic: %s" % topic)
    print("%s Key: %s; Value: %s" % (timestamp, key, val_json))
    print("******")

    review = val_json["review"]
    rating = val_json["rating"]

    reviews_processed_total.inc()
    # Upload a review with a rating
    review_rating.labels(review=review).set(rating)
    # TODO: potentialy transform to push to Prometheus so upload only once
    # it will update the existing metric with the new value instead of creating a new metric.
    # pushadd_to_gateway('prometheus:9090', job='hotel-reviews', registry=registry, grouping_key={'review': review})

running = True
def consume_loop(consumer: Consumer, topics: List[str]):
    """Subscribe to Kafka topic,

    Args:
        consumer (Consumer): initialized Kafka Consumer
        topics (List[str]): list of topics to subscribe

    Raises:
        KafkaException: failed to consume the message
    """
    try:
        consumer.subscribe(topics)

        msg_count = 0
        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                upload_to_prometheus(msg)
                msg_count += 1
                if msg_count % MIN_COMMIT_COUNT == 0:
                    consumer.commit(asynchronous=True)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == "__main__":
    print("Wating for Kafka to start")
    sleep(15)
    # start the Prometheus HTTP server
    start_http_server(8222)

    consumer = Consumer(conf)
    # start the Kafka consumer loop
    consume_loop(consumer, ["hotel-reviews-ratings"])