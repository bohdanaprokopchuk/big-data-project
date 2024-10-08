from pyspark.sql.types import StructType, StructField, StringType, IntegerType


def basic_test_df(spark_session):
    data = [("Anna", 18), ("Nina", 44), ("Karina", 27)]

    schema = StructType([
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True)
    ])

    df = spark_session.createDataFrame(data, schema)
    return df
