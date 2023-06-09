from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as f

"""
Read a DataFrame from the Spark Stream
"""
class HotelReviewStreamReader(object):

    def __init__(self, sparkSession: SparkSession) -> None:
        self.sparkSession = sparkSession
        self.hotelReviewSchema = StructType([
            StructField("review", StringType(), False),
        ])


    def read(self) -> DataFrame:
        """Read hotel reviews from the Kafka.

        Returns:
            DataFrame: initialized with values and schema
        """
        # subscribe to 1 topic defaults to the earliest and latest offsets
        df = self.sparkSession \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("subscribe", "hotel-reviews") \
            .option("startingOffsets", "earliest") \
            .option("failOnDataLoss", "false") \
            .load() \
            .withColumn("value", f.from_json(f.col("value").cast("string"), self.hotelReviewSchema)) \
            .withColumn("review", f.col("value.review")) \
            .filter(f.col("review").isNotNull())

        # Use Kafka's timestamp as sensor time,
        # to be able to perform timing statistics
        # Note that this isn't the real sensor time,
        # but the timestamp when the message was written to Kafka
        df = df \
            .withColumn("sensor_time", f.to_timestamp(f.col("timestamp")))

        return df
