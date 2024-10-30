from pyspark.sql import functions as f


def full_analysis(df, df_name):
    print(f"--- Analysis for {df_name} ---\n")

    print("Schema:")
    df.printSchema()
    print("\n")

    print("Basic statistics (describe):")
    df.describe().show()
    print("\n")

    print("Extended statistics (summary):")
    df.summary().show()
    print("\n")

    print("Missing values (%):")
    df.select([(f.count(f.when(f.col(c).isNull(), c)) / df.count() * 100).alias(c) for c in df.columns]).show()
    print("\n")

    print("Unique values:")
    for col in df.columns:
        unique_count = df.select(col).distinct().count()
        print(f"{col}: {unique_count} unique values")
    print("\n")

    print("Sample records:")
    df.show(5)
    print("\n")
