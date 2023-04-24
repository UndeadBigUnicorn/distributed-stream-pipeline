from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

"""
Load the hotel review dataset using the schema.
"""
class DatasetLoader(object):

    def __init__(self, sparkSession: SparkSession) -> None:
        self.sparkSession = sparkSession
        self.hotelReviewSchema = StructType([
            StructField("review", StringType(), False),
            StructField("rating", IntegerType(), False)
        ])

    def load(self, path: str) -> DataFrame:
        """Load the csv into a DataFrame using a schema.

        Args:
            path (str): to the csv file

        Returns:
            DataFrame: populated with data from csv file
        """
        return self.sparkSession.read.csv(path, sep = ",", header = True, schema = self.hotelReviewSchema)