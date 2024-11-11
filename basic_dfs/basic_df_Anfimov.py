from pyspark.sql import SparkSession

def basic_test_df_Anfimov():
    spark = SparkSession.builder \
        .appName("PySpark Example") \
        .getOrCreate()

    data = [("Alice", 25), ("Bob", 30), ("Charlie", 35)]
    columns = ["name", "age"]
    df = spark.createDataFrame(data, columns)

    return df
