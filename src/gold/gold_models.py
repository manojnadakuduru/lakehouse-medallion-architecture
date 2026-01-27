from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.builder.getOrCreate()


def read_silver_table():
    """
    Reads cleaned data from the silver table.
    """
    return spark.table("datamodeling.silver.silver_table")


def build_fact_sales(df):
    """
    Builds a fact table with sales metrics.
    """
    return df.select(
        col("order_id"),
        col("order_date"),
        col("customer_id"),
        col("product_id"),
        col("quantity"),
        col("unit_price"),
        expr("quantity * unit_price").alias("total_amount")
    )


def write_fact_table(df):
    """
    Writes fact table to gold layer.
    """
    df.write.mode("overwrite").saveAsTable("datamodeling.gold.fact_sales")


def main():
    silver_df = read_silver_table()
    fact_df = build_fact_sales(silver_df)
    write_fact_table(fact_df)


if __name__ == "__main__":
    main()
