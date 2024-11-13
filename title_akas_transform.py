from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when
from pyspark.sql.types import BooleanType


def transform_title_akas(df: DataFrame) -> DataFrame:
    """
    Transforms the title_akas DataFrame by:
    1. Renaming columns to snake_case.
    2. Converting 'isOriginalTitle' column to boolean.
    3. Replacing missing values with None.

    Args:
        df (DataFrame): Input DataFrame for the 'title.akas' dataset.

    Returns:
        DataFrame: Transformed DataFrame.
    """
    # Rename columns to snake_case
    df = df.withColumnRenamed("titleId", "title_id") \
        .withColumnRenamed("isOriginalTitle", "is_original_title")

    # Convert 'is_original_title' to boolean (1 = True, 0 = False)
    df = df.withColumn("is_original_title", col("is_original_title").cast(BooleanType()))

    # Replace '\N' with None in all columns
    df = df.replace('\\N', None)

    return df
