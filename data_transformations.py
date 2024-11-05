from pyspark.sql import DataFrame
from pyspark.sql.functions import col
from pyspark.sql import functions as f

def filter_movies_by_language(title_akas_df: DataFrame, language: str) -> DataFrame:
    """
    Filters movies released in a specific language.

    :param title_akas_df: DataFrame containing movie information with language column.
    :param language: Language code to filter movies by (e.g., "en" for English).
    :return: DataFrame containing movies released in the specified language.
    """
    return title_akas_df.filter(col("language") == language)


def filter_animation_movies(title_basics_df: DataFrame) -> DataFrame:
    """
    Filters movies classified under the "Animation" genre.

    :param title_basics_df: DataFrame containing movie information with genres.
    :return: DataFrame containing only movies in the "Animation" genre.
    """
    return title_basics_df.filter(col("genres").contains("Animation"))


def filter_movies_no_director(title_crew_df: DataFrame) -> DataFrame:
    """
    Filters movies without an assigned director.

    :param title_crew_df: DataFrame containing movie crew information with directors column.
    :return: DataFrame containing movies with no assigned director.
    """
    return title_crew_df.filter(col("directors").isNull())


def filter_short_films(title_basics_df: DataFrame) -> DataFrame:
    """
    Filters movies marked as "short films".

    :param title_basics_df: DataFrame containing movie information with title_type.
    :return: DataFrame containing movies marked as short films.
    """
    return title_basics_df.filter(col("title_type") == "short")


def filter_individuals_multiple_professions(name_basics_df: DataFrame) -> DataFrame:
    """
    Filters individuals with multiple listed professions.

    :param name_basics_df: DataFrame containing individual information with primary_profession.
    :return: DataFrame containing individuals with multiple professions.
    """
    return name_basics_df.filter(col("primary_profession").contains(","))


def filter_us_non_english_movies(title_akas_df: DataFrame) -> DataFrame:
    """
    Filters movies with "United States" as the primary region but not in English.

    :param title_akas_df: DataFrame containing movie information with region and language.
    :return: DataFrame containing movies with region "US" but not in English.
    """
    return title_akas_df.filter((col("region") == "US") & (col("language") != "en"))


def filter_movies_no_release_year(title_basics_df: DataFrame) -> DataFrame:
    """
    Filters movies with no specified release year.

    :param title_basics_df: DataFrame containing movie information with start_year.
    :return: DataFrame containing movies with missing release year information.
    """
    return title_basics_df.filter(col("start_year").isNull())


def filter_multi_genre_movies(title_basics_df: DataFrame) -> DataFrame:
    """
    Filters movies that belong to multiple genres.

    :param title_basics_df: DataFrame containing movie information with genres.
    :return: DataFrame containing movies that have multiple genres listed.
    """
    return title_basics_df.filter(col("genres").contains(","))


def count_movies_by_country(title_akas_df: DataFrame, country: str) -> int:
    """
    Filters movies released in a specific country and counts them.

    :param title_akas_df: DataFrame containing movie information with country column.
    :param country: The country code to filter movies by (e.g., "US" for United States).
    :return: The count of movies released in the specified country.
    """
    filtered_movies = title_akas_df.filter(col("region") == country)
    return filtered_movies.count()


