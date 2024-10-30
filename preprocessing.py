from pyspark.sql import DataFrame

def drop_column(df: DataFrame, column_name: str) -> DataFrame:
    """
    Drops a specified column from the DataFrame.

    Args:
        df (DataFrame): The input DataFrame.
        column_name (str): The name of the column to be dropped.

    Returns:
        DataFrame: The modified DataFrame without the specified column.
    """
    return df.drop(column_name)

def remove_duplicates(df: DataFrame) -> DataFrame:
    """
    Removes duplicate rows from the DataFrame.

    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with duplicate rows removed.
    """
    return df.dropDuplicates()