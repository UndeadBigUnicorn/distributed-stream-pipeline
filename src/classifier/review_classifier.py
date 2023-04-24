from pyspark.sql import SparkSession, DataFrame
from pyspark.ml.feature import StopWordsRemover, RegexTokenizer, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline, PipelineModel, Transformer

"""
Train the Classifier of hotel reviews
"""
class ReviewClassifier(object):

    def __init__(self, sparkSession: SparkSession) -> None:
        self.sparkSession = sparkSession
        # would be initialized later
        self.model = None

    def __call__(self, df: DataFrame) -> DataFrame:
        """Make a prediction using the trained model.

        Args:
            df (DataFrame): hotel review text to predict the rating

        Returns:
            DataFrame: hotel review with predicted rating
        """
        if self.model is None:
            raise Exception("Model should be initialized before calling it")

        # make predictions
        prediction = self.model.transform(df)
        return prediction

    def fit(self, df: DataFrame) -> None:
        """
        Train the logistic classifier of hotel reviews:
        1. Tokenize words and remove unnecessary chars
        2. Remove stop words
        3. Encode tf_idf
        4. Train Logistic Regression

        Args:
            df (DataFrame): hotel review DataFrame
        """
        # setup tokenizer
        tokenizer = RegexTokenizer()\
            .setInputCol("review")\
            .setOutputCol("tokens")\
            .setPattern("([’']s)?[,!?\"'`:;.—-“”‘’]*\\s+[,!?\"'`:;.—-“”‘’]*")\
            .setMinTokenLength(2)

        # remove stop words
        stopWords = StopWordsRemover()\
            .setStopWords(StopWordsRemover.loadDefaultStopWords("english"))\
            .setInputCol("tokens")\
            .setOutputCol("tokens_clean")

        # encode tf
        hashingTF = HashingTF()\
            .setInputCol("tokens_clean")\
            .setOutputCol("tf")

        # encode idf
        idf = IDF()\
            .setInputCol("tf")\
            .setOutputCol("features")

        # use logistic regression
        lr = LogisticRegression(maxIter=100, regParam=0.001, elasticNetParam=0.8,
                                labelCol="rating")

        # prepare pipeline
        predictingPipeline = Pipeline(
            stages=[tokenizer, stopWords, hashingTF, idf, lr])

        # fit the pipeline to training documents.
        self.model = predictingPipeline.fit(df)

    def save(self, path: str) -> None:
        """Save the model to the path

        Args:
            path (str): to save the model
        """
        self.model.save(path)

    def load(self, path: str) -> None:
        """Load the model from the path.

        Args:
            path (str): to saved model
        """
        self.model = PipelineModel.load(path)
