from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, udf
from pyspark.sql.types import ArrayType, StringType
import json


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
    df = df.withColumnRenamed("tconst", "title_id") \
        .withColumnRenamed("nconst", "person_id")

    # Replace with None in all applicable columns
    df = df.select([when(col(c) == "\\N", None).otherwise(col(c)).alias(c) for c in df.columns])

    # Convert 'characters' column from string to array
    def parse_characters(characters):
        # noinspection PyBroadException
        try:
            return json.loads(characters)
        except:
            return None

    parse_characters_udf = udf(parse_characters, ArrayType(StringType()))
    df = df.withColumn("characters", parse_characters_udf(col("characters")))

    return df
