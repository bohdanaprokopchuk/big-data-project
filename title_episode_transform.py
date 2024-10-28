import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, StringType


def transform_title_episode(df):
    """
    Transforms the title episode DataFrame by renaming columns to snake_case,
    replacing missing values with null values, and ensuring data types are appropriate.

    Parameters:
    - df: PySpark DataFrame containing the raw data from title.episode.tsv.gz

    Returns:
    - Transformed PySpark DataFrame
    """
    # Rename columns to snake_case
    df = df.withColumnRenamed("tconst", "tconst") \
        .withColumnRenamed("parentTconst", "parent_tconst") \
        .withColumnRenamed("seasonNumber", "season_number") \
        .withColumnRenamed("episodeNumber", "episode_number")

    # Handle missing values
    df = df.replace('\\N', None)

    # Ensure data types are appropriate
    df = df.withColumn("season_number", f.col("season_number").cast(IntegerType())) \
        .withColumn("episode_number", f.col("episode_number").cast(IntegerType()))

    return df
