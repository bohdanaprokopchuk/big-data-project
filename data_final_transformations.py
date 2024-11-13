from pyspark.sql import Window
from pyspark.sql import functions as f
from pyspark.sql.functions import col, desc, row_number



def get_lowest_rated_movies(title_ratings, title_akas):
    """
    Returns movies with the lowest ratings and their titles.

    Parameters:
    - title_ratings: DataFrame containing movie ratings.
    - title_akas: DataFrame containing general information, such as titles of movies.

    Returns:
    - DataFrame with movie titles and their lowest ratings.
    """

    title_ratings = title_ratings.alias("ratings")
    title_akas = title_akas.alias("akas")

    return (
        title_ratings
        .join(
            title_akas,
            title_ratings.t_const == title_akas.title_id,
            'left'
        )
        .select("akas.title", "ratings.t_const", "ratings.average_rating")
        .groupBy("akas.title", "ratings.t_const")
        .agg(f.min("ratings.average_rating").alias("lowest_rating"))
        .orderBy("lowest_rating")
    )



def get_most_reviewed_movies(title_ratings, title_akas):
    """
    Returns movies with the highest number of reviews, and their titles, using a window function for ranking.

    Parameters:
    - title_ratings: DataFrame containing movie ratings.
    - title_akas: DataFrame containing alternative titles of movies.

    Returns:
    - DataFrame with movie title, movie ID, number of reviews, and ranking.
    """

    title_ratings = title_ratings.alias("ratings")
    title_akas = title_akas.alias("akas")

    joined_df = (
        title_ratings.join(
            title_akas,
            title_ratings.t_const == title_akas.title_id,
            'inner'
        )
        .filter(f.col("akas.is_original_title") == True)
    )

    window_spec = Window.orderBy(f.desc("ratings.num_votes"))

    return (
        joined_df
        .select("akas.title", "ratings.t_const", "ratings.num_votes")
        .withColumn("review_rank", f.rank().over(window_spec))
        .orderBy("review_rank")
    )



def get_actor_avg_rating(title_principals, title_ratings):
    """
    Returns actors with the highest average ratings for the movies they appeared in.

    Parameters:
    - title_principals: DataFrame with information about actors in movies.
    - title_ratings: DataFrame containing movie ratings.

    Returns:
    - DataFrame with actor IDs and their average ratings.
    """
    return (
        title_principals.join(title_ratings, title_principals.title_id == title_ratings.tconst, 'inner')
        .groupBy("person_id")
        .agg(f.avg("average_rating").alias("avg_actor_rating"))
        .orderBy(f.desc("avg_actor_rating"))
    )



def get_longest_movie_runtime(title_basics, title_ratings):
    """
    Returns the longest movie by runtime and its rating.

    Parameters:
    - title_basics: DataFrame containing movie information, including runtime.
    - title_ratings: DataFrame containing movie ratings.

    Returns:
    - DataFrame with the longest movie's title, runtime, and rating, sorted by runtime.
    """
    window_spec = Window.orderBy(f.desc("runtime_minutes"))

    return (
        title_basics.join(title_ratings, "t_const", "inner")
        .select("primary_title", "runtime_minutes", "average_rating")
        .withColumn("rank", f.rank().over(window_spec))
        .filter(f.col("rank") == 1)
    )



def top_directors_by_movie_rating(title_crew, title_ratings):
    """
    Returns directors with the highest average movie ratings.

    Parameters:
    - title_crew: DataFrame with director information for each movie.
    - title_ratings: DataFrame containing movie ratings.

    Returns:
    - DataFrame with directors and their average movie ratings, sorted in descending order.
    """
    director_ratings = title_crew \
        .withColumn("director_id", f.explode(f.split("directors", ","))) \
        .join(title_ratings, title_crew.tconst == title_ratings.tconst, 'inner') \
        .groupBy("director_id") \
        .agg(f.avg("average_rating").alias("avg_director_rating")) \
        .orderBy(f.desc("avg_director_rating"))

    return director_ratings



def get_top_series_by_episode_count(title_basics, title_episode):
    """
        This function filters the series from the `title_basics` table, retrieves their identifiers,
        and counts the episodes for each series from the `title_episode` table.

        Parameters:
        title_basics : containing basic title information, including the `t_const` and `primary_title` columns.
        title_episode : containing episode information, including the `parent_t_const` column that links episodes to series.

        Returns:
        DataFrame with two columns: `primary_title` (series title) and `count` (number of episodes),
            sorted in descending order by episode count.
        """

    series_titles = title_basics.filter(col("title_type") == "tvSeries") \
        .select("t_const", "primary_title")

    episode_count = title_episode.groupBy("parent_t_const") \
        .count() \
        .join(series_titles, series_titles["t_const"] == col("parent_t_const")) \
        .select("primary_title", "count") \
        .orderBy(desc("count"))
    return episode_count



def top_directors_by_movie_count(title_crew, name_basics):
    """
    Returns directors who have directed the most movies.

    Args:
        title_crew (DataFrame): DataFrame containing information about movie directors.
        name_basics (DataFrame): DataFrame with details about actors/directors.

    Returns:
        DataFrame: Names of directors and the count of movies they directed.
    """
    director_df = title_crew \
        .withColumn("directors", f.explode(f.split(f.col("directors"), ',')))

    director_movie_count = director_df.groupBy("directors").count()

    result = director_movie_count \
        .join(name_basics, name_basics.n_const == director_movie_count.directors, "inner") \
        .select("primary_name", "count") \
        .orderBy(f.desc("count"))

    return result



def top_movies_per_year(title_basics, title_ratings):
    """
    Returns a DataFrame of the top-rated movie for each year.

    Parameters:
    title_basics : containing title information, including `t_const`, `title_type`, and `start_year`.
    title_ratings : containing rating information, including `t_const` and `average_rating`.

    Returns:
    DataFrame with columns: `start_year`, `primary_title`, and `average_rating`, listing the top-rated movie for each year.
    """
    movies = title_basics.filter(col("title_type") == "movie") \
        .select("t_const", "primary_title", "start_year")

    movie_ratings = movies.join(title_ratings, on="t_const", how="inner") \
        .select("start_year", "primary_title", "average_rating")

    year_window = Window.partitionBy("start_year").orderBy(col("average_rating").desc())

    top_movies = movie_ratings.withColumn("rank", row_number().over(year_window)) \
        .filter(col("rank") == 1) \
        .select("start_year", "primary_title", "average_rating")

    return top_movies