from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.getOrCreate()


def read_bronze_table():
    """
    Reads data from the bronze table.
    """
    return spark.table("datamodeling.bronze.bronze_table")


def clean_data(df):
    """
    Applies basic cleaning rules:
    - Drop duplicate orders
    - Remove records with null order_id
    """
    return (
        df.dropDuplicates(["order_id"])
          .filter(col("order_id").isNotNull())
    )


def write_silver_table(df):
    """
    Writes cleaned data to the silver table.
    """
    df.write.mode("overwrite").saveAsTable("datamodeling.silver.silver_table")


def main():
    bronze_df = read_bronze_table()
    cleaned_df = clean_data(bronze_df)
    write_silver_table(cleaned_df)


if __name__ == "__main__":
    main()
