#!/bin/bash

pip install numpy

/opt/bitnami/spark/bin/spark-submit --conf spark.driver.port=10099 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.2 --master local[2] --executor-memory 8G --num-executors 2 --name StatisticsStream statistics_stream.py
