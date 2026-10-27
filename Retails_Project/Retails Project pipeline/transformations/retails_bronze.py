
from pyspark import pipelines as dp
from pyspark.sql.functions import current_timestamp


@dp.materialized_view(name="customers_bronze_test")
def customers_bronze_test():
    customer_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/retails_project/raw_data/raw_file/retail_customers_messy.csv")
    )

    return customer_df.withColumn("ingest_time", current_timestamp())


@dp.materialized_view(name="orders_bronze_test")
def orders_bronze_test():
    orders_df = (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/retails_project/raw_data/raw_file/retail_orders_messy.csv")
    )

    return orders_df.withColumn("ingest_time", current_timestamp())
