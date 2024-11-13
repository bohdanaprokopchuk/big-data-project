from pyspark.sql import Window
from pyspark.sql import functions as f



def get_most_films_by_country(title_akas):
    """
    Calculates and returns the number of films produced in each country, sorted in descending order by the number of films.

    Args:
        title_akas (DataFrame): A DataFrame containing movie information, including regional data.

    Returns:
        DataFrame: A DataFrame with two columns:
                   - `region`: The country code representing the film's production region.
                   - `movie_count`: The number of films produced in each country.
                   Sorted by `movie_count` in descending order to show the countries with the most films first.
    """

    return (
        title_akas.filter(f.col("region").isNotNull())
        .groupBy("region")
        .agg(
            f.count("title_id").alias("movie_count")
        )
        .orderBy(f.desc("movie_count"))
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



def yearly_genre_growth(title_basics):
    """
    Calculates the year-over-year increase in the number of movies produced for each genre.

    Parameters:
    - title_basics: DataFrame with movie genres and release years.

    Returns:
    - DataFrame with each genre, year, and the yearly increase in the number of movies produced.
    """

    genre_growth = title_basics \
        .withColumn("genre", f.explode(f.split("genres", ","))) \
        .groupBy("start_year", "genre") \
        .count() \
        .withColumnRenamed("count", "current_count")


    window_spec = Window.partitionBy("genre").orderBy("start_year")

    genre_growth_with_prev = genre_growth \
        .withColumn("previous_count", f.lag("current_count", 1).over(window_spec)) \
        .withColumn("yearly_increase", f.col("current_count") - f.coalesce(f.col("previous_count"), f.lit(0))) \
        .select("start_year", "genre", "yearly_increase") \
        .orderBy("genre", "start_year")

    return genre_growth_with_prev



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



def count_movies_by_genre_per_year_with_titles_window(title_basics):
    """
    Count the number of movies for each genre per year and display their original titles, using window functions.

    Args:
        title_basics (DataFrame): Contains genre, original title, and release year information for movies.

    Returns:
        DataFrame: Number of movies per genre per year, sorted by the number of movies,
                  along with the original titles of the movies.
    """

    title_basics_single_genre = title_basics.withColumn("genres", f.explode(f.split(f.col("genres"), ",")))

    window_spec = Window.partitionBy("start_year", "genres").orderBy("original_title")

    title_basics_with_row_num = title_basics_single_genre.withColumn("row_num", f.row_number().over(window_spec))

    unique_titles = title_basics_with_row_num.filter(f.col("row_num") == 1)

    genre_year_count = unique_titles.groupBy("start_year", "genres").count()

    title_collection = unique_titles.groupBy("start_year", "genres").agg(
        f.collect_list("original_title").alias("original_titles")
    )

    result = genre_year_count.join(title_collection, on=["start_year", "genres"])

    return result.orderBy(f.desc("count"))



def count_movies_by_genre(title_basics):
    """
    Returns a DataFrame with the count of movies for each genre.

    Parameters:
    title_basics : DataFrame containing title information, including `t_const`, `title_type`, and `genres`.

    Returns:
    DataFrame with columns: `genre`, `movie_count`, showing the number of movies in each genre.
    """
    genre_count = title_basics.filter(f.col("title_type") == "movie") \
        .select("genres") \
        .withColumn("genre", f.explode(f.split(f.col("genres"), ","))) \
        .groupBy("genre") \
        .agg(f.count("genre").alias("movie_count")) \
        .orderBy(f.desc("movie_count"))

    return genre_count
