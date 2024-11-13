from pyspark.sql import DataFrame
import pyspark.sql.functions as f
from pyspark.sql.types import StringType


def transform_title_principals(df: DataFrame) -> DataFrame:
    """
    Transforms the title_principals DataFrame by:
    1. Renaming columns to snake_case.
    2. Parsing the 'characters' column as an array.
    3. Replacing missing values with None.

    Args:
        df (DataFrame): Input DataFrame for the 'title.principals' dataset.

    Returns:
        DataFrame: Transformed DataFrame.
    """

    # Rename columns to snake_case
    df = df.withColumnRenamed("tconst", "t_const") \
        .withColumnRenamed("nconst", "n_const")

    # Replace "\N" to None in all columns
    df = df.replace('\\N', None)

    # Ensure data types are strings, as they hold comma-separated lists of 'characters'
    df = df.withColumn("characters", f.col("characters").cast(StringType()))

    return df
