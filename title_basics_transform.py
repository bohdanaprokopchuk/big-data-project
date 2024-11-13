import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType, BooleanType


def transform_title_basics(df):
    """
    Transforms the title basics DataFrame by renaming columns to snake_case,
    replacing missing values with null values, and setting appropriate data types.

    Parameters:
    - df: PySpark DataFrame containing the raw data from title.basics.tsv.gz

    Returns:
    - Transformed PySpark DataFrame
    """
    # Rename columns to snake_case
    df = df.withColumnRenamed("tconst", "t_const") \
        .withColumnRenamed("titleType", "title_type") \
        .withColumnRenamed("primaryTitle", "primary_title") \
        .withColumnRenamed("originalTitle", "original_title") \
        .withColumnRenamed("isAdult", "is_adult") \
        .withColumnRenamed("startYear", "start_year") \
        .withColumnRenamed("endYear", "end_year") \
        .withColumnRenamed("runtimeMinutes", "runtime_minutes") \
        .withColumnRenamed("genres", "genres")

    # Handle missing values
    df = df.replace('\\N', None)

    # Convert data types
    df = df.withColumn("is_adult", f.col("is_adult").cast(BooleanType())) \
        .withColumn("start_year", f.col("start_year").cast(IntegerType())) \
        .withColumn("end_year", f.col("end_year").cast(IntegerType())) \
        .withColumn("runtime_minutes", f.col("runtime_minutes").cast(IntegerType())) \
        .withColumn("genres", f.col("genres").cast(StringType()))

    return df