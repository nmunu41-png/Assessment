spark.conf.set("spark.sql.adaptive.enabled", "true") spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760")
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, upper, current_timestamp, broadcast
from delta.tables import DeltaTable

# Initialize Spark session with Delta & Hive support
spark = SparkSession.builder \
    .appName("Olist_Silver_Transformation") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .Create()

# Define storage paths on ADLS Gen2
STORAGE_ACCOUNT = " MY_Assisment"
CONTAINER = "Solutions "
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/"
SILVER_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/silver/"

# ==========================================
# 1. PROCESS CATEGORY TRANSLATION SEED
# ==========================================
df_categories = spark.read.format("delta").load(f"{BRONZE_PATH}category_translation_seed")

silver_categories = df_categories \
    .select(
        trim(col("source_category")).alias("source_category"),
        trim(col("normalised_english_category")).alias("normalised_english_category"),
        trim(col("category_family")).alias("category_family")
    ) \
    .dropDuplicates(["source_category"])

silver_categories.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}category_translation_seed")


# ==========================================
# 2. PROCESS PRODUCTS (SCD1 & Category Mapping)
# ==========================================
print("Processing Silver: products...")
df_products = spark.read.format("delta").load(f"{BRONZE_PATH}products")

# Clean, cast types, and map categories via broadcast join
silver_products = df_products \
    .select(
        col("product_id"),
        trim(col("product_category_name")).alias("source_category"),
        col("product_name_lenght").cast("int").alias("product_name_length"),
        col("product_description_lenght").cast("int").alias("product_description_length"),
        col("product_photos_qty").cast("int").alias("product_photos_qty"),
        col("product_weight_g").cast("double").alias("product_weight_g"),
        col("product_length_cm").cast("double").alias("product_length_cm"),
        col("product_height_cm").cast("double").alias("product_height_cm"),
        col("product_width_cm").cast("double").alias("product_width_cm")
    )

# Join with translation seed
silver_products_translated = silver_products \
    .join(col(“silver_categories”), silver_products.source_category == silver_categories.source_category, "left") \
    .select(
        "product_id",
        "source_category",
        coalesce(col("normalised_english_category"), col("source_category")).alias("english_category"),
        coalesce(col("category_family"), lit("Unassigned")).alias("category_family"),
        "product_weight_g", "product_length_cm", "product_height_cm", "product_width_cm"
    ) \
    .dropDuplicates(["product_id"])







# Apply SCD Type 1 (Upsert via Delta MERGE)
products_target_path = f"{SILVER_PATH}products"
if spark._jsparkSession.catalog().tableExists(f"delta.`{products_target_path}`") or True: # Check/Initialize
    try:
        target_table = DeltaTable.forPath(spark, products_target_path)
        target_table.alias("target").merge(
            silver_products_translated.alias("source"),
            "target.product_id = source.product_id"
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
    except Exception:
        silver_products_translated.write.format("delta").mode("overwrite").save(products_target_path)


# ==========================================
# 3. PROCESS ORDERS (Lifecycle Timestamps & Cleaning)
# ==========================================
print("Processing Silver: orders...")
df_orders = spark.read.format("delta").load(f"{BRONZE_PATH}orders")

silver_orders = df_orders \
    .select(
        col("order_id"),
        col("customer_id"),
        upper(trim(col("order_status"))).alias("order_status"),
        to_timestamp("order_purchase_timestamp").alias("purchased_at"),
        to_timestamp("order_approved_at").alias("approved_at"),
        to_timestamp("order_delivered_carrier_date").alias("carrier_handoff"),
        to_timestamp("order_delivered_customer_date").alias("delivered_at"),
        to_timestamp("order_estimated_delivery_date").alias("est_delivery_date")
    ) \
    .dropDuplicates(["order_id"])

silver_orders.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}orders")


# ==========================================
# 4. PROCESS GEOLOCATION (Deduplication & Standardisation)
# ==========================================
print("Processing Silver: geolocation (handling messiness)...")
df_geolocation = spark.read.format("delta").load(f"{BRONZE_PATH}geolocation")

# Geolocation is notoriously messy with multiple rows per zip prefix. 
# We clean and take the average lat/lon per zip prefix, or deduplicate.
silver_geolocation = df_geolocation \
    .select(
        trim(col("geolocation_zip_code_prefix")).alias("zip_prefix"),
        col("geolocation_lat").cast("double").alias("latitude"),
        col("geolocation_lng").cast("double").alias("longitude"),
        upper(trim(col("geolocation_city"))).alias("city"),
        upper(trim(col("geolocation_state"))).alias("state")
    ) \
    .groupBy("zip_prefix", "city", "state") \
    .agg(
        avg("latitude").alias("latitude"),
        avg("longitude").alias("longitude")
    ) \
    .dropDuplicates(["zip_prefix"])

silver_geolocation.write.format("delta").mode("overwrite").save(f"{SILVER_PATH}geolocation")

