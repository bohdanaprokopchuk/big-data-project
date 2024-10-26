from pyspark import SparkConf
from pyspark.sql import SparkSession
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
    name_basics_df.printSchema()  # Вивести схему
    name_basics_df.show(5)         # Вивести перші 5 рядків
    write_name_basics_df_to_csv(name_basics_df, path="data/results/name_basics_csv", mode="overwrite", num_partitions=2)

    title_akas_df = read_title_akas_df(spark_session)
    title_akas_df.printSchema()     # Вивести схему
    title_akas_df.show(5)           # Вивести перші 5 рядків
    write_title_akas_df_to_csv(title_akas_df, path="data/results/title_akas_csv", mode="overwrite", num_partitions=2)

    title_basics_df = read_title_basics_df(spark_session)
    title_basics_df.printSchema()   # Вивести схему
    title_basics_df.show(5)         # Вивести перші 5 рядків
    write_title_basics_df_to_csv(title_basics_df, path="data/results/title_basics_csv", mode="overwrite", num_partitions=2)

    title_crew_df = read_title_crew_df(spark_session)
    title_crew_df.printSchema()      # Вивести схему
    title_crew_df.show(5)            # Вивести перші 5 рядків
    write_title_crew_df_to_csv(title_crew_df, path="data/results/title_crew_csv", mode="overwrite", num_partitions=2)

    title_episode_df = read_title_episode_df(spark_session)
    title_episode_df.printSchema()    # Вивести схему
    title_episode_df.show(5)          # Вивести перші 5 рядків
    write_title_episode_df_to_csv(title_episode_df, path="data/results/title_episode_csv", mode="overwrite", num_partitions=2)

    title_principals_df = read_title_principals_df(spark_session)
    title_principals_df.printSchema()  # Вивести схему
    title_principals_df.show(5)        # Вивести перші 5 рядків
    write_title_principals_df_to_csv(title_principals_df, path="data/results/title_principals_csv", mode="overwrite", num_partitions=2)

    title_ratings_df = read_title_ratings_df(spark_session)
    title_ratings_df.printSchema()     # Вивести схему
    title_ratings_df.show(5)           # Вивести перші 5 рядків
    write_title_ratings_df_to_csv(title_ratings_df, path="data/results/title_ratings_csv", mode="overwrite", num_partitions=2)

spark_session.stop()