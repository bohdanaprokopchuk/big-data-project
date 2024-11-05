from pyspark.shell import spark
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as f


def get_lowest_rated_movies(title_ratings, title_principals):
    """
    Returns movies with the lowest ratings and the actors who starred in them.

    Parameters:
    - title_ratings: DataFrame containing movie ratings.
    - title_principals: DataFrame containing information about movie participants.

    Returns:
    - DataFrame with movie IDs, their lowest ratings, and actors.
    """
    return (
        title_ratings.join(title_principals, title_ratings.tconst == title_principals.title_id, 'inner')
        .select("title_id", "average_rating", "person_id")
        .groupBy("title_id", "person_id")
        .agg(f.min("average_rating").alias("lowest_rating"))
        .orderBy("lowest_rating")
    )


def get_most_films_by_country(title_akas):
    """
    Returns countries that produce the most films.

    Parameters:
    - title_akas: DataFrame containing regional information about movies.

    Returns:
    - DataFrame with country names and the number of movies, sorted in descending order.
    """
    return (
        title_akas.groupBy("region")
        .count()
        .orderBy(f.desc("count"))
    )


def get_avg_ratings_by_year(title_basics, title_ratings):
    """
    Returns average movie ratings by year.

    Parameters:
    - title_basics: DataFrame with movie information, including release year.
    - title_ratings: DataFrame containing movie ratings.

    Returns:
    - DataFrame with years and average ratings, sorted by year.
    """
    return (
        title_basics.join(title_ratings, title_basics.tconst == title_ratings.tconst, 'inner')
        .groupBy("start_year")
        .agg(f.avg("average_rating").alias("avg_rating"))
        .orderBy("start_year")
    )


def get_most_reviewed_movies(title_ratings):
    """
    Returns movies with the highest number of reviews, using a window function for ranking.

    Parameters:
    - title_ratings: DataFrame containing movie ratings.

    Returns:
    - DataFrame with movies, number of reviews, and ranking based on the number of votes.
    """
    window_spec = Window.orderBy(f.desc("num_votes"))
    return (
        title_ratings.withColumn("review_rank", f.rank().over(window_spec))
        .orderBy("review_rank")
    )


def get_drama_percentage(title_basics):
    """
    Returns the percentage of movies that belong to the "Drama" genre.

    Parameters:
    - title_basics: DataFrame containing movie information, including genres.

    Returns:
    - Float value showing the percentage of movies in the "Drama" genre.
    """
    total_count = title_basics.count()
    drama_count = title_basics.filter(title_basics.genres.contains("Drama")).count()
    return (drama_count / total_count) * 100


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
        title_basics.join(title_ratings, "tconst", "inner")
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


def top_actors_by_movie_count(title_principals, name_basics):
    """
    Returns actors who have appeared in the most movies.

    Parameters:
    - title_principals: DataFrame with information about actors in movies.
    - name_basics: DataFrame containing actor details.

    Returns:
    - DataFrame with actor names and the count of movies they appeared in.
    """
    actor_movie_counts = title_principals \
        .groupBy("person_id") \
        .count() \
        .join(name_basics, title_principals.person_id == name_basics.nconst, 'inner') \
        .select("primary_name", "count") \
        .orderBy(f.desc("count"))

    return actor_movie_counts


def yearly_genre_growth(title_basics):
    """
    Calculates the year-over-year increase in the number of movies produced for each genre.

    Parameters:
    - title_basics: DataFrame with movie genres and release years.

    Returns:
    - DataFrame with each genre, year, and the yearly increase in the number of movies produced.
    """
    window_spec = Window.partitionBy("genre").orderBy("start_year")

    genre_growth = title_basics \
        .withColumn("genre", f.explode(f.split("genres", ","))) \
        .groupBy("start_year", "genre") \
        .count() \
        .withColumn("previous_count", f.lag("count", 1).over(window_spec)) \
        .withColumn("yearly_increase", f.col("count") - f.coalesce(f.col("previous_count"), f.lit(0))) \
        .select("start_year", "genre", "yearly_increase") \
        .orderBy("genre", "start_year")

    return genre_growth


def get_family_friendly_movies(title_basics):
    """
    Filters for family-friendly movies in specific genres ("Adventure," "Animation," or "Fantasy").

    Parameters:
    - title_basics: DataFrame containing movie information, including genres and adult content flag.

    Returns:
    - DataFrame with family-friendly movies in the specified genres.
    """
    target_genres = ["Adventure", "Animation", "Fantasy"]

    return (
        title_basics
        .filter((title_basics.is_adult == 0) & (title_basics.genres.isin(target_genres)))
        .select("primary_title", "genres", "start_year")
        .orderBy("start_year", ascending=False)
    )