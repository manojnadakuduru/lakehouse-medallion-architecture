from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_date

spark = SparkSession.builder.getOrCreate()


def read_silver_table():
    """
    Reads the latest cleaned data from the silver layer.
    """
    return spark.table("datamodeling.silver.silver_table")


def read_dimension_table():
    """
    Reads the existing dimension table if it exists.
    """
    if spark.catalog.tableExists("datamodeling.gold.dim_customer"):
        return spark.table("datamodeling.gold.dim_customer")
    return None


def apply_scd_type2(source_df, target_df):
    """
    Applies Slowly Changing Dimension Type 2 logic.
    """
    if target_df is None:
        return (
            source_df
            .withColumn("effective_date", current_date())
            .withColumn("end_date", lit(None))
            .withColumn("is_current", lit(True))
        )

    updates = (
        source_df.alias("src")
        .join(target_df.alias("tgt"), "customer_id")
        .filter(col("tgt.is_current") == True)
        .filter(col("src.customer_name") != col("tgt.customer_name"))
    )

    expired = (
        updates.select("tgt.*")
        .withColumn("end_date", current_date())
        .withColumn("is_current", lit(False))
    )

    new_versions = (
        updates.select("src.*")
        .withColumn("effective_date", current_date())
        .withColumn("end_date", lit(None))
        .withColumn("is_current", lit(True))
    )

    unchanged = target_df.join(
        updates.select("customer_id"), "customer_id", "left_anti"
    )

    return unchanged.union(expired).union(new_versions)


def write_dimension_table(df):
    """
    Writes the updated dimension table.
    """
    df.write.mode("overwrite").saveAsTable("datamodeling.gold.dim_customer")


def main():
    source_df = read_silver_table()
    target_df = read_dimension_table()
    final_df = apply_scd_type2(source_df, target_df)
    write_dimension_table(final_df)


if __name__ == "__main__":
    main()
