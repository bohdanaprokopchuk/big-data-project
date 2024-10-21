from pyspark import SparkConf
from pyspark.sql import SparkSession
from io_data import read_name_basics_df

spark_session = (SparkSession.builder
                             .master("local")
                             .appName("NameBasicsProcessing")
                             .config(conf=SparkConf())
                             .getOrCreate())


if __name__ == "__main__":
    df = read_name_basics_df(spark_session)
