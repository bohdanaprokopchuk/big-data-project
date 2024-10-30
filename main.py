from pyspark import SparkConf
from pyspark.sql import SparkSession
from preprocessing import drop_column, remove_duplicates
import data_transformations as tr

from io_data import (
    read_name_basics_df, write_name_basics_df_to_csv,
    read_title_akas_df, write_title_akas_df_to_csv,
    read_title_basics_df, write_title_basics_df_to_csv,
    read_title_crew_df, write_title_crew_df_to_csv,
    read_title_episode_df, write_title_episode_df_to_csv,
    read_title_principals_df, write_title_principals_df_to_csv,
    read_title_ratings_df, write_title_ratings_df_to_csv
)
from name_basics_transform import transform_name_basics
from title_akas_transform import transform_title_akas
from title_basics_transform import transform_title_basics
from title_crew_transform import transform_title_crew
from title_episode_transform import transform_title_episode
from title_principals_transform import transform_title_principals
from title_ratings_transform import transform_title_ratings
from full_analysis import full_analysis

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

    transformed_dfs = {
        "name_basics": transformed_name_basics_df,
        "title_akas": transformed_title_akas_df,
        "title_basics": transformed_title_basics_df,
        "title_crew": transformed_title_crew_df,
        "title_episode": transformed_title_episode_df,
        "title_principals": transformed_title_principals_df,
        "title_ratings": transformed_title_ratings_df
    }

    for df_name, df in transformed_dfs.items():
        if df_name == "title_akas":
            df = drop_column(df, "attributes")
        df = remove_duplicates(df)

        transformed_dfs[df_name] = df

    movies_in_english = tr.filter_movies_by_language(transformed_dfs["title_akas"], "en")
    movies_in_english.write.csv("data/results/movies_in_english.csv", header=True)


    animation_movies = tr.filter_animation_movies(transformed_dfs["title_basics"])
    animation_movies.write.csv("data/results/animation_movies.csv", header=True)


    movies_no_director = tr.filter_movies_no_director(transformed_dfs["title_crew"])
    movies_no_director.write.csv("data/results/movies_no_director.csv", header=True)


    short_films = tr.filter_short_films(transformed_dfs["title_basics"])
    short_films.write.csv("data/results/short_films.csv", header=True)


    individuals_multiple_professions = tr.filter_individuals_multiple_professions(transformed_dfs["name_basics"])
    individuals_multiple_professions.write.csv("data/results/individuals_multiple_professions.csv", header=True)


    movies_no_release_year = tr.filter_movies_no_release_year(transformed_dfs["title_basics"])
    movies_no_release_year.write.csv("data/results/movies_no_release_year.csv", header=True)


    multi_genre_movies = tr.filter_multi_genre_movies(transformed_dfs["title_basics"])
    multi_genre_movies.write.csv("data/results/multi_genre_movies.csv", header=True)

    for df_name, df in transformed_dfs.items():
        print(f"--- Analysis for {df_name} ---")
        full_analysis(df, df_name)

    write_name_basics_df_to_csv(transformed_name_basics_df, path="data/results/name_basics_csv", mode="overwrite", num_partitions=2)
    write_title_akas_df_to_csv(transformed_title_akas_df, path="data/results/title_akas_csv", mode="overwrite", num_partitions=2)
    write_title_basics_df_to_csv(transformed_title_basics_df, path="data/results/title_basics_csv", mode="overwrite", num_partitions=2)
    write_title_crew_df_to_csv(transformed_title_crew_df, path="data/results/title_crew_csv", mode="overwrite", num_partitions=2)
    write_title_episode_df_to_csv(transformed_title_episode_df, path="data/results/title_episode_csv", mode="overwrite", num_partitions=2)
    write_title_principals_df_to_csv(transformed_title_principals_df, path="data/results/title_principals_csv", mode="overwrite", num_partitions=2)
    write_title_ratings_df_to_csv(transformed_title_ratings_df, path="data/results/title_ratings_csv", mode="overwrite", num_partitions=2)
spark_session.stop()