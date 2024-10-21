from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from settings import FILE_PATH, OUTPUT_PATH


name_basics_schema = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", IntegerType(), True),
    StructField("deathYear", IntegerType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
])


def read_name_basics_df(spark_session, path=FILE_PATH):
    return spark_session.read.csv(
        path,
        sep='\t',
        header=True,
        schema=name_basics_schema,
        inferSchema=False,
        mode='DROPMALFORMED',
        nullValue='\\N',
        quote='"',
        escape="\\",
        dateFormat='yyyy'
    )


def write_name_basics_df_to_csv(df: DataFrame, path=OUTPUT_PATH, mode="overwrite", num_partitions=1):
    df.repartition(num_partitions).write.csv(path, header=True, mode=mode)
