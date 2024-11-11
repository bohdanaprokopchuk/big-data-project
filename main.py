from pyspark import SparkConf
from pyspark.sql import SparkSession
from basic_dfs.basic_df_Anfimov import basic_test_df_Anfimov

spark_session = (SparkSession.builder
                             .master("local")
                             .appName("task app")
                             .config(conf=SparkConf())
                             .getOrCreate())

df = basic_test_df_Anfimov()
df.show()
