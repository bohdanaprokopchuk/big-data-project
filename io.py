from pyspark.sql import SparkSession
from settings import FILE_PATH

spark = SparkSession.builder.appName('TitleRatingsProcessing').getOrCreate()


def read_title_ratings_df(path=FILE_PATH):
    return spark.read.csv(path, sep='\t', header=True)
