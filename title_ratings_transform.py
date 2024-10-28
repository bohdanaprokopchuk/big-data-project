from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.types import FloatType, IntegerType

def transform_title_ratings(df: DataFrame) -> DataFrame:
    """
    Transforms the 'title.ratings.tsv.gz' DataFrame by renaming columns,
    ensuring type consistency, and handling missing values.

    Parameters:
    df (DataFrame): Input DataFrame containing movie ratings data.

    Returns:
    DataFrame: Transformed DataFrame with consistent schema.
    """
    # Rename columns to snake_case
    df = df.withColumnRenamed("averageRating", "average_rating") \
           .withColumnRenamed("numVotes", "num_votes")

    # Ensure correct data types
    df = df.withColumn("average_rating", col("average_rating").cast(FloatType())) \
           .withColumn("num_votes", col("num_votes").cast(IntegerType()))

    # Handle missing values
    df = df.fillna({"average_rating": 0.0, "num_votes": 0})

    return df
