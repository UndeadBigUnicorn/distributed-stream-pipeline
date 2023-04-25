#!/bin/bash

pip install numpy

/opt/bitnami/spark/bin/spark-submit --conf spark.driver.port=9099 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 --master local[2] --executor-memory 8G --num-executors 2 --py-files classifier/review_classifier.py,stream/stream_listener.py,stream/stream_reader.py,stream/stream_writer.py --name HotelReviewStream hotel_review_stream.py
