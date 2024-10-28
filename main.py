from pyspark import SparkConf
from pyspark.sql import SparkSession
from name_basics_transform import transform_name_basics
from title_akas_transform import transform_title_akas
from title_basics_transform import transform_title_basics
from title_crew_transform import transform_title_crew
from title_episode_transform import transform_title_episode
from title_principals_transform import transform_title_principals
from title_ratings_transform import transform_title_ratings

from io_data import (
    read_name_basics_df, write_name_basics_df_to_csv,
    read_title_akas_df, write_title_akas_df_to_csv,
    read_title_basics_df, write_title_basics_df_to_csv,
    read_title_crew_df, write_title_crew_df_to_csv,
    read_title_episode_df, write_title_episode_df_to_csv,
    read_title_principals_df, write_title_principals_df_to_csv,
    read_title_ratings_df, write_title_ratings_df_to_csv
)

spark_session = (SparkSession.builder
                             .master("local")
                             .appName("IMDB Data Processing")
                             .config(conf=SparkConf())
                             .getOrCreate())

if __name__ == "__main__":
    name_basics_df = read_name_basics_df(spark_session)
    title_akas_df = read_title_akas_df(spark_session)
    title_basics_df = read_title_basics_df(spark_session)
    title_crew_df = read_title_crew_df(spark_session)
    title_episode_df = read_title_episode_df(spark_session)
    title_principals_df = read_title_principals_df(spark_session)
    title_ratings_df = read_title_ratings_df(spark_session)

    transformed_name_basics_df = transform_name_basics(name_basics_df)
    transformed_title_akas_df = transform_title_akas(title_akas_df)
    transformed_title_basics_df = transform_title_basics(title_basics_df)
    transformed_title_crew_df = transform_title_crew(title_crew_df)
    transformed_title_episode_df = transform_title_episode(title_episode_df)
    transformed_title_principals_df = transform_title_principals(title_principals_df)
    transformed_title_ratings_df = transform_title_ratings(title_ratings_df)

    write_name_basics_df_to_csv(transformed_name_basics_df, path="data/results/name_basics_csv", mode="overwrite", num_partitions=2)
    write_title_akas_df_to_csv(transformed_title_akas_df, path="data/results/title_akas_csv", mode="overwrite", num_partitions=2)
    write_title_basics_df_to_csv(transformed_title_basics_df, path="data/results/title_basics_csv", mode="overwrite", num_partitions=2)
    write_title_crew_df_to_csv(transformed_title_crew_df, path="data/results/title_crew_csv", mode="overwrite", num_partitions=2)
    write_title_episode_df_to_csv(transformed_title_episode_df, path="data/results/title_episode_csv", mode="overwrite", num_partitions=2)
    write_title_principals_df_to_csv(transformed_title_principals_df, path="data/results/title_principals_csv", mode="overwrite", num_partitions=2)
    write_title_ratings_df_to_csv(transformed_title_ratings_df, path="data/results/title_ratings_csv", mode="overwrite", num_partitions=2)
spark_session.stop()