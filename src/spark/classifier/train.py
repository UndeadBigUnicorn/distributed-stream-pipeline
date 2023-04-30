#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Train the Logistic Regression model.

Usage: spark-submit
"""

from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from dataset_loader import DatasetLoader
from review_classifier import ReviewClassifier

if __name__ == "__main__":

    print("Initializing the Spark Connection...")

    # initialise Spark Session
    sparkSession = SparkSession.builder.appName("HotelReviewClassifier")\
        .master("local[8]")\
        .config("spark.memory.fraction", 0.8) \
        .config("spark.driver.cores", 8) \
        .config("spark.executor.memory", "8g") \
        .config("spark.driver.memory", "8g")\
        .config("spark.sql.shuffle.partitions", "800") \
        .getOrCreate()
    # change log level to WARN
    sparkSession.sparkContext.setLogLevel("WARN")

    # load the dataset
    datasetLoader = DatasetLoader(sparkSession)
    hotelReviewsDF = datasetLoader.load("/data/train_data.csv")

    print("Training the model...")

    # train the model
    reviewClassifier = ReviewClassifier(sparkSession)
    reviewClassifier.fit(hotelReviewsDF)
    reviewClassifier.save("/data/hotel-classifier")

    print("Evaluating the model...")

    # evaluate the model
    evaluator = MulticlassClassificationEvaluator()\
        .setLabelCol("rating")\
        .setPredictionCol("prediction")\
        .setMetricName("weightedPrecision")\
        .setMetricName("weightedRecall")\
        .setMetricName("f1")

    testDf = datasetLoader.load("/data/test_data.csv")
    predictedTestDF = reviewClassifier(testDf)
    # Using weighted metrics to account for class imbalance
    precision = evaluator.evaluate(predictedTestDF,
                                   {evaluator.metricName: "weightedPrecision"})
    recall = evaluator.evaluate(predictedTestDF,
                                {evaluator.metricName: "weightedRecall"})
    f1_score = evaluator.evaluate(predictedTestDF,
                                  {evaluator.metricName: "f1"})

    print("Precision on the test dataset: %f" % precision)
    print("Recall on the test dataset: %f" % recall)
    print("F1 score on the test dataset: %f" % f1_score)
