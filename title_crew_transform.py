import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType


def transform_title_crew(df):
    """
    Transforms the title crew DataFrame by renaming columns to snake_case,
    replacing missing values with null values, and ensuring data types are appropriate.

    Parameters:
    - df: PySpark DataFrame containing the raw data from title.crew.tsv.gz

    Returns:
    - Transformed PySpark DataFrame
    """
    # Rename columns to snake_case
    df = df.withColumnRenamed("tconst", "t_const") \
        .withColumnRenamed("directors", "directors") \
        .withColumnRenamed("writers", "writers")

    # Handle missing values
    df = df.replace('\\N', None)

    # Ensure data types are strings, as they hold comma-separated lists of IDs
    df = df.withColumn("directors", f.col("directors").cast(StringType())) \
        .withColumn("writers", f.col("writers").cast(StringType()))

    return df
