#!/bin/bash

pip install numpy

/opt/bitnami/spark/bin/spark-submit --conf spark.driver.port=9099 --master local[8] --executor-memory 8G --num-executors 2 --py-files dataset_loader.py,review_classifier.py --name HotelReviewClassifier train.py
