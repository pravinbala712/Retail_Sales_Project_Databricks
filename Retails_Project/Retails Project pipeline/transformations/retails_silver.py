from pyspark import pipelines as dp
from pyspark.sql.functions import *
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window

@dp.materialized_view(name="retails_project.silver.orders_silver_test")
def orders_silver_test():
    orders = spark.read.table(
        "retails_project.bronze.orders_bronze_test"
    )

    orders = orders.withColumn(
        "order_date",
        coalesce(
            try_to_date(col("order_date"), "yyyy-MM-dd"),
            try_to_date(col("order_date"), "dd-MM-yyyy"),
            try_to_date(col("order_date"), "yyyy/MM/dd"),
            try_to_date(col("order_date"), "dd/MM/yyyy")
        )
    )

    orders = orders.withColumn(
        "category",
        upper(trim(col("category")))
    )

    orders = orders.withColumn(
        "category",
        when(col("category") == "ELECTRONIC", "ELECTRONICS")
        .when(col("category") == "HOME AND LIVING", "HOME & LIVING")
        .when(col("category") == "FASHON", "FASHION")
        .otherwise(col("category"))
    )

    orders = orders.dropDuplicates(["order_id"])

    orders = orders.withColumn(
        "discount",
        when(col("discount").isNull(), "0")
        .when(col("discount") == "ten", "0.1")
        .otherwise(col("discount"))
    )

    orders = orders.withColumn(
        "discount",
        when(col("discount").like("5%"),
             regexp_replace(col("discount"), "5%", "0.05"))
        .when(col("discount").like("10%"),
             regexp_replace(col("discount"), "10%", "0.1"))
        .otherwise(col("discount"))
    )

    orders = orders.filter(col("quantity") >= 0)

    orders = orders.withColumn(
        "unit_price",
        col("unit_price").try_cast(DecimalType(10, 2))
    )

    orders = orders.withColumn(
        "total_amount",
        col("quantity").cast("int")
        * col("unit_price")
        * (1 - col("discount").cast("double"))
    )

    return orders


# Silver Customers
@dp.materialized_view(name="retails_project.silver.customers_silver_test")
def customers_silver_test():
    customers = spark.read.table(
        "retails_project.bronze.customers_bronze_test"
    )

    customers = customers.select(
        trim(col("customer_id")).alias("customer_id"),
        trim(col("customer_name")).alias("customer_name"),
        trim(col("email")).alias("email"),
        trim(col("phone")).alias("phone"),
        trim(col("gender")).alias("gender"),
        trim(col("date_of_birth")).alias("date_of_birth"),
        trim(col("city")).alias("city"),
        trim(col("state")).alias("state"),
        trim(col("registration_date")).alias("registration_date"),
        trim(col("loyalty_points")).alias("loyalty_points"),
        trim(col("status")).alias("status"),
        col("ingest_time")
    )

    window_spec = Window.partitionBy("customer_id").orderBy(
        desc("ingest_time")
    )

    customers = (
        customers.withColumn("row_num", row_number().over(window_spec))
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    customers = customers.withColumn("status", upper(col("status")))

    customers = customers.withColumn(
        "status",
        when(col("status").isin("ACTIVE", "INACTIVE"), col("status"))
        .otherwise("UNKNOWN")
    )

    customers = customers.withColumn(
        "loyalty_points",
        regexp_replace(col("loyalty_points"), r"[^0-9]", "")
    ).withColumn(
        "loyalty_points",
        col("loyalty_points").try_cast("int")
    )

    customers = customers.withColumn(
        "email_valid",
        col("email").rlike(r"^[A-Za-z0-9+_.-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")
    )

    customers = customers.withColumn(
        "dob_parsed",
        coalesce(
            try_to_date(col("date_of_birth"), "yyyy-MM-dd"),
            try_to_date(col("date_of_birth"), "dd-MM-yyyy"),
            try_to_date(col("date_of_birth"), "yyyy/MM/dd"),
            try_to_date(col("date_of_birth"), "dd/MM/yyyy")
        )
    )

    customers = customers.withColumn(
        "registration_parsed",
        coalesce(
            try_to_date(col("registration_date"), "yyyy-MM-dd"),
            try_to_date(col("registration_date"), "dd-MM-yyyy"),
            try_to_date(col("registration_date"), "yyyy/MM/dd"),
            try_to_date(col("registration_date"), "dd/MM/yyyy")
        )
    )

    customers = (
        customers.drop("date_of_birth", "registration_date")
        .withColumnRenamed("dob_parsed", "date_of_birth")
        .withColumnRenamed("registration_parsed", "registration_date")
    )

    customers = customers.withColumn("gender", upper(col("gender")))

    customers = customers.withColumn(
        "gender",
        when(col("gender") == "MALE", "M")
        .when(col("gender") == "FEMALE", "F")
        .otherwise("U")
    )

    return customers

