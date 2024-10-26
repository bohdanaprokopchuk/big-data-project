from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from settings import (FILE_PATH_NAME_BASICS, FILE_PATH_TITLE_AKAS,
                      FILE_PATH_TITLE_BASICS, FILE_PATH_TITLE_CREW,
                      FILE_PATH_TITLE_EPISODE, FILE_PATH_TITLE_PRINCIPALS,
                      FILE_PATH_TITLE_RATINGS, OUTPUT_PATH_NAME_BASICS,
                      OUTPUT_PATH_TITLE_AKAS, OUTPUT_PATH_TITLE_BASICS,
                      OUTPUT_PATH_TITLE_CREW, OUTPUT_PATH_TITLE_EPISODE,
                      OUTPUT_PATH_TITLE_PRINCIPALS, OUTPUT_PATH_TITLE_RATINGS)


name_basics_schema = StructType([
    StructField("nconst", StringType(), True),
    StructField("primaryName", StringType(), True),
    StructField("birthYear", IntegerType(), True),
    StructField("deathYear", IntegerType(), True),
    StructField("primaryProfession", StringType(), True),
    StructField("knownForTitles", StringType(), True)
])


def read_name_basics_df(spark_session, path=FILE_PATH_NAME_BASICS):
    return spark_session.read.csv(
        path,
        sep='\t',
        header=True,
        schema=name_basics_schema,
        inferSchema=False,
        mode='DROPMALFORMED',
        nullValue='\\N',
        quote='"',
        escape="\\",
        dateFormat='yyyy'
    )


def write_name_basics_df_to_csv(df: DataFrame, path=OUTPUT_PATH_NAME_BASICS, mode="overwrite", num_partitions=1):
    df.repartition(num_partitions).write.csv(path, header=True, mode=mode)


title_akas_schema = StructType([
    StructField("titleId", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("title", StringType(), True),
    StructField("region", StringType(), True),
    StructField("language", StringType(), True),
    StructField("types", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("isOriginalTitle", IntegerType(), True)
])

def read_title_akas_df(spark_session, path=FILE_PATH_TITLE_AKAS):
    return spark_session.read.csv(
        path,
        sep='\t',
        header=True,
        schema=title_akas_schema,
        inferSchema=False,
        mode='DROPMALFORMED',
        nullValue='\\N'
    )

def write_title_akas_df_to_csv(df: DataFrame, path=OUTPUT_PATH_TITLE_AKAS, mode="overwrite", num_partitions=1):
    df.repartition(num_partitions).write.csv(path, header=True, mode=mode)

title_basics_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("titleType", StringType(), True),
    StructField("primaryTitle", StringType(), True),
    StructField("originalTitle", StringType(), True),
    StructField("isAdult", IntegerType(), True),
    StructField("startYear", IntegerType(), True),
    StructField("endYear", IntegerType(), True),
    StructField("runtimeMinutes", IntegerType(), True),
    StructField("genres", StringType(), True)
])

def read_title_basics_df(spark_session, path=FILE_PATH_TITLE_BASICS):
    return spark_session.read.csv(
        path,
        sep='\t',
        header=True,
        schema=title_basics_schema,
        inferSchema=False,
        mode='DROPMALFORMED',
        nullValue='\\N'
    )

def write_title_basics_df_to_csv(df: DataFrame, path=OUTPUT_PATH_TITLE_BASICS, mode="overwrite", num_partitions=1):
    df.repartition(num_partitions).write.csv(path, header=True, mode=mode)


title_crew_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("directors", StringType(), True),
    StructField("writers", StringType(), True)
])

def read_title_crew_df(spark_session, path=FILE_PATH_TITLE_CREW):
    return spark_session.read.csv(
        path,
        sep='\t',
        header=True,
        schema=title_crew_schema,
        inferSchema=False,
        mode='DROPMALFORMED',
        nullValue='\\N'
    )

def write_title_crew_df_to_csv(df: DataFrame, path=OUTPUT_PATH_TITLE_CREW, mode="overwrite", num_partitions=1):
    df.repartition(num_partitions).write.csv(path, header=True, mode=mode)


title_episode_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("parentTconst", StringType(), True),
    StructField("seasonNumber", StringType(), True),
    StructField("episodeNumber", StringType(), True)
])

def read_title_episode_df(spark_session, path=FILE_PATH_TITLE_EPISODE):
    return spark_session.read.csv(
        path,
        sep='\t',
        header=True,
        schema=title_episode_schema,
        inferSchema=False,
        mode='DROPMALFORMED',
        nullValue='\\N'
    )

def write_title_episode_df_to_csv(df: DataFrame, path=OUTPUT_PATH_TITLE_EPISODE, mode="overwrite", num_partitions=1):
    df.repartition(num_partitions).write.csv(path, header=True, mode=mode)


title_principals_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("ordering", IntegerType(), True),
    StructField("nconst", StringType(), True),
    StructField("category", StringType(), True),
    StructField("job", StringType(), True),
    StructField("characters", StringType(), True)
])

def read_title_principals_df(spark_session, path=FILE_PATH_TITLE_PRINCIPALS):
    return spark_session.read.csv(
        path,
        sep='\t',
        header=True,
        schema=title_principals_schema,
        inferSchema=False,
        mode='DROPMALFORMED',
        nullValue='\\N'
    )

def write_title_principals_df_to_csv(df: DataFrame, path=OUTPUT_PATH_TITLE_PRINCIPALS, mode="overwrite", num_partitions=1):
    df.repartition(num_partitions).write.csv(path, header=True, mode=mode)


title_ratings_schema = StructType([
    StructField("tconst", StringType(), True),
    StructField("averageRating", DoubleType(), True),
    StructField("numVotes", IntegerType(), True)
])

def read_title_ratings_df(spark_session, path=FILE_PATH_TITLE_RATINGS):
    return spark_session.read.csv(
        path,
        sep='\t',
        header=True,
        schema=title_ratings_schema,
        inferSchema=False,
        mode='DROPMALFORMED',
        nullValue='\\N'
    )

def write_title_ratings_df_to_csv(df: DataFrame, path=OUTPUT_PATH_TITLE_RATINGS, mode="overwrite", num_partitions=1):
    df.repartition(num_partitions).write.csv(path, header=True, mode=mode)