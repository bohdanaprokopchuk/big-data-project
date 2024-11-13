from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, StringType


def transform_name_basics(df: DataFrame) -> DataFrame:
    """
    Transforms the `name.basics.tsv.gz` DataFrame by renaming columns to snake_case,
    handling missing values, and ensuring consistent data types.

    Args:
        df (DataFrame): The input DataFrame for the `name.basics.tsv.gz` data.

    Returns:
        DataFrame: The transformed DataFrame with columns in snake_case,
                   correct data types, and missing values handled.
    """
    # Rename columns to snake_case
    df = df.withColumnRenamed("nconst", "n_const") \
        .withColumnRenamed("primaryName", "primary_name") \
        .withColumnRenamed("birthYear", "birth_year") \
        .withColumnRenamed("deathYear", "death_year") \
        .withColumnRenamed("primaryProfession", "primary_profession") \
        .withColumnRenamed("knownForTitles", "known_for_titles")

    # Handle missing value
    df = df.replace('\\N', None)

    # Ensure data types are strings, as they hold comma-separated lists.
    df = df.withColumn("known_for_titles", f.col("known_for_titles").cast(StringType()))

    # Set correct data types
    df = df.withColumn("birth_year", col("birth_year").cast(IntegerType()))
    df = df.withColumn("death_year", col("death_year").cast(IntegerType()))

    return df


