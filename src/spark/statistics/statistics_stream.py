#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
    Create statistics from the output of the hotel review stream.
"""
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, \
    TimestampType, DoubleType
import pyspark.sql.functions as f
import uuid

if __name__ == "__main__":
    print("Initializing the Spark Connection for statistics_stream.py...")

    # Initialise Spark Session
    sparkSession = SparkSession.builder.appName("StatisticsStream") \
        .master("local[2]") \
        .getOrCreate()

    # Change log level to WARN, to avoid printing too many messages
    sparkSession.sparkContext.setLogLevel("WARN")

    # Export stream metrics
    sparkSession.conf.set("spark.sql.streaming.metricsEnabled", "true")

    # Read from Kafka topic hotel-reviews-ratings
    df = sparkSession \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "hotel-reviews-ratings") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Add processing time form the Kafka timestamp
    df = df \
        .withColumn("processing_time", f.col("timestamp"))

    # Transform the data
    # from {key: ..., value: {review: ..., rating: ...}}
    # to {review: ..., rating: ...}
    reviewsRatingsSchema = StructType([
        StructField("review", StringType(), False),
        StructField("rating", DoubleType(), False),
        StructField("sensor_time", TimestampType(), False)
    ])
    df = df \
        .withColumn("value",
                    f.from_json(f.col("value").cast("string"),
                                reviewsRatingsSchema)) \
        .withColumn("review", f.col("value.review")) \
        .withColumn("rating", f.col("value.rating")) \
        .withColumn("sensor_time", f.col("value.sensor_time")) \
        .select("review", "rating", "sensor_time", "processing_time")

    # Calculate the time difference between sensor time and processing time
    df = df \
        .withColumn("time_diff_ms",
                    (f.col("processing_time").cast("double")*1000 -
                     f.col("sensor_time").cast("double")*1000).cast("long")) \

    # Calculate review length
    df = df \
        .withColumn("review_length", f.length(f.col("review")))

    # Group the data with a sliding window with the window size of 5 minutes
    # and the slide interval of 30 seconds.
    # The window is defined by the processing time.
    df = df \
        .groupBy(f.window(f.col("processing_time"), "5 minutes", "30 seconds"))        

    # Create statistics from the transformed data
    df = df \
        .agg(f.count("*").alias("count"),
             f.min("review_length").alias("min_review_length"),
             f.max("review_length").alias("max_review_length"),
             f.avg("review_length").alias("avg_review_length"),
             f.stddev("review_length").alias("stddev_review_length"),
             f.min("rating").alias("min_rating"),
             f.max("rating").alias("max_rating"),
             f.avg("rating").alias("avg_rating"),
             f.stddev("rating").alias("stddev_rating"),
             f.min("time_diff_ms").alias("min_time_diff_ms"),
             f.max("time_diff_ms").alias("max_time_diff_ms"),
             f.avg("time_diff_ms").alias("avg_time_diff_ms"),
             f.stddev("time_diff_ms").alias("stddev_time_diff_ms"))

    # Select window start and end time, for easier reading, and the statistics
    df = df \
        .select(f.col("window.start").alias("window_start"),
                f.col("window.end").alias("window_end"), "count",
                "min_review_length", "max_review_length", "avg_review_length",
                "stddev_review_length", "min_rating", "max_rating",
                "avg_rating", "stddev_rating", "min_time_diff_ms",
                "max_time_diff_ms", "avg_time_diff_ms", "stddev_time_diff_ms")

    # Create function that generates a UUID
    uuidUdf = f.udf(lambda: str(uuid.uuid4()), StringType())

    # Convert data into Kafka message format
    # {key: ..., value: {window.start: ..., window.end: ..., count: ..., ...}}
    df = df \
        .withColumn("value",
                    f.to_json(f.struct([f.col(x) for x in df.columns]))) \
        .withColumn("key", uuidUdf()) \
        .select("key", "value")

    # Write the data to Kafka topic statistics
    query = df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("topic", "statistics") \
        .option("checkpointLocation", "/tmp/statistics") \
        .outputMode("complete") \
        .start()

    # Wait for the termination signal
    query.awaitTermination()
