from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

def get_last_load_date():
    """
    Returns the latest order_date from the bronze table.
    If the table does not exist, returns a very old date.
    """
    if spark.catalog.tableExists("datamodeling.bronze.bronze_table"):
        return spark.sql(
            "SELECT MAX(order_date) FROM datamodeling.bronze.bronze_table"
        ).collect()[0][0]
    else:
        return "1000-01-01"


def load_incremental_data(last_load_date):
    """
    Reads only new records from the source table.
    """
    return spark.sql(f"""
        SELECT *
        FROM datamodeling.default.source_data
        WHERE order_date > '{last_load_date}'
    """)


def write_bronze_table(df):
    """
    Writes data to the bronze table.
    """
    df.write.mode("append").saveAsTable("datamodeling.bronze.bronze_table")


def main():
    last_load_date = get_last_load_date()
    incremental_df = load_incremental_data(last_load_date)
    write_bronze_table(incremental_df)


if __name__ == "__main__":
    main()
