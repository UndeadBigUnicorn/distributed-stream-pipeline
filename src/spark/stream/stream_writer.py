from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StringType
from pyspark.sql import functions as f
from pyspark.sql.streaming import StreamingQuery
import uuid

"""
Write into stream
"""
class HotelReviewStreamWriter(object):

    def __init__(self, sparkSession: SparkSession) -> None:
        self.sparkSession = sparkSession

    def write(self, df: DataFrame) -> StreamingQuery:
        """Write predicted ratings for hotel reviews back to the Kafka.

        Args:
            df (DataFrame): predicted ratings for hotel reviews
        """
        uuidUdf = f.udf(lambda : str(uuid.uuid4()), StringType())
        # keep only 2 columns
        df = df \
            .withColumn("rating", f.col("prediction")) \
            .select(f.col("review"), f.col("rating"))
        # dataframe should have key, value
        # convert columns into 1 json value and generate a key
        return df \
            .withColumn("value", f.to_json(f.struct([f.col(x) for x in df.columns]))) \
            .withColumn("key", uuidUdf()) \
            .select(f.col("key"), f.col("value")) \
            .writeStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", "hotel-reviews-ratings") \
            .option("checkpointLocation", "/tmp/checkpoint") \
            .outputMode("append") \
            .start()
            # .option("asyncProgressTrackingEnabled", "true") \
            # .foreachBatch(lambda df, epoch_id: print((df.head(2)))) \
