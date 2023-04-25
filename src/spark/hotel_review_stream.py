#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Process hotel reviews using Apache Spark Streaming.

Usage: spark-submit
"""
from pyspark.sql import SparkSession
from stream.stream_reader import HotelReviewStreamReader
from stream.stream_writer import HotelReviewStreamWriter
# from stream.stream_listener import StreamListener
from classifier.review_classifier import ReviewClassifier

if __name__ == "__main__":

    print("Initializing the Spark Connection...")

    # initialise Spark Session
    sparkSession = SparkSession.builder.appName("ClassifierStream") \
        .master("local[2]") \
        .getOrCreate()
    # change log level to WARN
    sparkSession.sparkContext.setLogLevel("WARN")
    # export stream metrics
    sparkSession.conf.set("spark.sql.streaming.metricsEnabled", "true")

    # add stream listener
    # sparkSession.streams.addListener(StreamListener())

    # load the model
    reviewClassifier = ReviewClassifier(sparkSession)
    reviewClassifier.load("/data/hotel-classifier")

    # read from the stream
    streamReader = HotelReviewStreamReader(sparkSession)
    hotelReviewStream = streamReader.read()

    print("Query recieved")
    # print("# Elements in the batch, columns: ")
    # print((hotelReviewStream.count(), hotelReviewStream.columns))
    # print("First elements in the batch: ")
    # print((hotelReviewStream.head(2)))

    print("Predicting rating...")
    # predict rating
    hotelReviewRatingDF = reviewClassifier(hotelReviewStream)
    # print("# Elements in the batch, columns: ")
    # print((hotelReviewRatingDF.count(), hotelReviewRatingDF.columns))
    # print("First elements in the batch: ")
    # print((hotelReviewRatingDF.head(2)))

    # write back to Kafka
    streamWriter = HotelReviewStreamWriter(sparkSession)
    query = streamWriter.write(hotelReviewRatingDF)
    query.awaitTermination()
    print(query)
