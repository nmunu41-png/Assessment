from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, year, month, sum as spark_sum, count, avg, coalesce, lit, current_timestamp

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Olist_Gold_StarSchema") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define ADLS Gen2 paths
CONTAINER = "your_container_name"
STORAGE_ACCOUNT = "your_storage_account_name"
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/"
GOLD_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/gold/"

# Read Silver tables
df_orders = spark.read.format("delta").load(f"{SILVER_PATH}orders")
df_items = spark.read.format("delta").load(f"{SILVER_PATH}order_items") # Assumed created in silver
df_products = spark.read.format("delta").load(f"{SILVER_PATH}products")
df_customers = spark.read.format("delta").load(f"{SILVER_PATH}customers") # Assumed silver
df_sellers = spark.read.format("delta").load(f"{SILVER_PATH}sellers") # Assumed silver
df_payments = spark.read.format("delta").load(f"{SILVER_PATH}payments") # Assumed silver
df_reviews = spark.read.format("delta").load(f"{SILVER_PATH}reviews") # Assumed silver
df_geo = spark.read.format("delta").load(f"{SILVER_PATH}geolocation")



# 1. BUILD DIMENSIONS
# ==========================================

print("Building Gold Dimensions...")

# Dim Customer
dim_customer = df_customers \
    .join(df_geo, df_customers.customer_zip_prefix == df_geo.zip_prefix, "left") \
    .select(
        col("customer_id"),
        col("customer_unique_id"),
        col("customer_zip_prefix").alias("zip_prefix"),
        coalesce(col("city"), col("customer_city")).alias("city"),
        coalesce(col("state"), col("customer_state")).alias("state"),
        col("latitude"),
        col("longitude")
    ) \
    .dropDuplicates(["customer_id"])

dim_customer.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}dim_customer")

# Dim Seller
dim_seller = df_sellers \
    .join(df_geo, df_sellers.seller_zip_prefix == df_geo.zip_prefix, "left") \
    .select(
        col("seller_id"),
        col("seller_zip_prefix").alias("zip_prefix"),
        coalesce(col("city"), col("seller_city")).alias("city"),
        coalesce(col("state"), col("seller_state")).alias("state"),
        col("latitude"),
        col("longitude")
    ) \
    .dropDuplicates(["seller_id"])

dim_seller.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}dim_seller")

# Dim Product
dim_product = df_products \
    .select(
        col("product_id"),
        col("english_category"),
        col("category_family"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm")
    ) \
    .dropDuplicates(["product_id"])

dim_product.write.format("delta").mode("overwrite").save(f"{GOLD_PATH}dim_product")


# ==========================================
# 2. BUILD FACT TABLE (Fact Sales / Order Items)
# ==========================================

print("Building Gold Fact Table...")

# Aggregate payments per order to handle multiple payments/installments safely at the order grain
agg_payments = df_payments \
    .groupBy("order_id") \
    .agg(
        spark_sum("payment_value").alias("total_payment_value"),
        count("payment_sequential").alias("payment_installments_count")
    )

# Aggregate reviews per order (taking the first or average score if multiple)
agg_reviews = df_reviews \
    .groupBy("order_id") \
    .agg(
        avg("review_score").alias("avg_review_score"),
        count("review_id").alias("review_count")
    )

# Construct Fact Order Items (Item-level grain provides maximum analytical flexibility)
fact_sales = df_items \
    .join(df_orders, "order_id", "inner") \
    .join(agg_payments, "order_id", "left") \
    .join(agg_reviews, "order_id", "left") \
    .select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("customer_id"),
        col("seller_id"),
        col("order_status"),
        col("purchased_at"),
        to_date(col("purchased_at")).alias("purchase_date"),
        col("approved_at"),
        col("carrier_handoff"),
        col("delivered_at"),
        col("est_delivery_date"),
        col("item_price"),
        col("freight_cost"),
        (col("item_price") + col("freight_cost")).alias("total_item_value"),
        coalesce(col("avg_review_score"), lit(0.0)).alias("review_score"),
        col("total_payment_value")
    ) \
    .withColumn("partition_year_month", date_format(col("purchased_at"), "yyyy-MM"))

# Write Fact Table partitioned by Year-Month for optimal query pruning
fact_sales.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("partition_year_month") \
    .save(f"{GOLD_PATH}fact_sales")

print("Gold layer star schema pipeline completed successfully.")
