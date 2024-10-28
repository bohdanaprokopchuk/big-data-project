from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when


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
    df = df.withColumnRenamed("nconst", "nconst") \
        .withColumnRenamed("primaryName", "primary_name") \
        .withColumnRenamed("birthYear", "birth_year") \
        .withColumnRenamed("deathYear", "death_year") \
        .withColumnRenamed("primaryProfession", "primary_profession") \
        .withColumnRenamed("knownForTitles", "known_for_titles")

    # Handle missing values and set data types
    df = df.withColumn("birth_year", when(col("birth_year") == "\\N", None).otherwise(col("birth_year").cast("int"))) \
        .withColumn("death_year", when(col("death_year") == "\\N", None).otherwise(col("death_year").cast("int")))

    return df


