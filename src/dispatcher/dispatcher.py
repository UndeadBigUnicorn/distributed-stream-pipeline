#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Consume hotel review statistics from Apache Kafka streams and upload it into Prometheus.

Usage: dispatcher.py
"""
import sys
import json
from time import sleep
from typing import List
from confluent_kafka import Consumer, Message, KafkaError, KafkaException
from prometheus_client import start_http_server, Counter, Gauge

# Define Kafka properties
conf = {
    "bootstrap.servers": "kafka:29092",
    "group.id": "data.dispatcher",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

MIN_COMMIT_COUNT = 5

# Define Prometheus metrics
reviews_processed_total = Counter("hotel_reviews_ratings_processed_total", "Total number of messages processed")
review_min_length = Gauge("hotel_review_min_review_length", "Min length of the review")
review_max_length = Gauge("hotel_review_max_review_length", "Max length of the review")
review_avg_length = Gauge("hotel_review_avg_review_length", "Avg length of the review")
review_avg_rating = Gauge("hotel_review_avg_review_rating", "Avg rating of the review")
review_min_processing_time_ms = Gauge("hotel_review_min_processing_time_ms", "Min processing time of the review")
review_max_processing_time_ms = Gauge("hotel_review_max_processing_time_ms", "Max processing time of the review")
review_avg_processing_time_ms = Gauge("hotel_review_avg_processing_time_ms", "Avg processing time of the review")

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

    # update metrics
    try:
        reviews_processed_total.inc()
        review_min_length.set(val_json["min_review_length"])
        review_max_length.set(val_json["max_review_length"])
        review_avg_length.set(val_json["avg_review_length"])
        review_avg_rating.set(val_json["avg_rating"])
        review_min_processing_time_ms.set(val_json["min_time_diff_ms"])
        review_max_processing_time_ms.set(val_json["max_time_diff_ms"])
        review_avg_processing_time_ms.set(val_json["avg_time_diff_ms"])
    except Exception as e:
        print(e)

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
    consume_loop(consumer, ["statistics"])