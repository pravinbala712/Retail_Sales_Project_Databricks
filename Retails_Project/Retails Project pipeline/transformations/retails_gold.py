from pyspark import pipelines as dp
from pyspark.sql.functions import *

@dp.materialized_view(
    name="retails_project.gold.gold_dim_customer_test"
)
def gold_dim_customer_test():
    customers = spark.read.table(
        "retails_project.silver.customers_silver_test"
    )

    return customers.select(
        "customer_id",
        "customer_name",
        "email",
        "city",
        "state",
        "loyalty_points",
        "status"
    )


# Gold Sales Fact
@dp.materialized_view(
    name="retails_project.gold.gold_fact_sales_test"
)
def gold_fact_sales_test():
    orders = spark.read.table(
        "retails_project.silver.orders_silver_test"
    )

    customers = spark.read.table(
        "retails_project.silver.customers_silver_test"
    )

    enriched = orders.join(
        customers,
        orders.customer_id == customers.customer_id,
        "left"
    ).select(
        orders.order_id,
        orders.order_date,
        orders.customer_id,
        customers.customer_name,
        orders.product_id,
        orders.product_name,
        orders.category,
        orders.quantity,
        orders.unit_price,
        orders.discount,
        orders.total_amount,
        customers.city,
        customers.state,
        customers.gender,
        customers.loyalty_points
    )

    enriched = (
        enriched
        .withColumn("Year", year("order_date"))
        .withColumn("Month", month("order_date"))
        .withColumn("Day", dayofmonth("order_date"))
    )

    return enriched.groupBy(
        "product_id",
        "product_name",
        "customer_id",
        "customer_name",
        "category",
        "Year",
        "Month",
        "order_date",
        "gender",
        "city",
        "state",
        "loyalty_points"
    ).agg(
        sum("total_amount").alias("Total_Revenue"),
        sum("quantity").alias("Total_Quantity"),
        countDistinct("order_id").alias("Total_Orders"),
        avg("total_amount").alias("Avg_Revenue"),
        avg("quantity").alias("Avg_Quantity"),
        avg("loyalty_points").alias("Avg_Loyalty"),
        min("unit_price").alias("Min_Price"),
        max("unit_price").alias("Max_Price")
    )
