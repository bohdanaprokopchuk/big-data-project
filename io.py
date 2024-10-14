from pyspark.sql import SparkSession
from settings import FILE_PATH

spark = SparkSession.builder.appName('NameBasicsProcessing').getOrCreate()


def read_name_basics_df(path=FILE_PATH):
    return spark.read.csv(path, sep='\t', header=True)
