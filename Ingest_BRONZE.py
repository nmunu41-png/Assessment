
"""To ingest your Olist dataset into the Bronze layer using Azure Data Lake Storage Gen2 (ADLS Gen2)"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, input_file_name

# Initialize Spark session with Delta Lake support
spark = SparkSession.builder \
    .appName("Olist_Bronze_ADLS_Ingestion") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define ADLS Gen2 Storage Account and Container configurations
STORAGE_ACCOUNT = " MY_Assisment"
CONTAINER = "Solutions "

# Configure Spark to use OAuth or Access Keys for ADLS Gen2 authentication
# (manage credentials via Databricks secrets)
spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.dfs.core.windows.net", "your-service-principal-client-id")
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.dfs.core.windows.net", "your-service-principal-secret")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.dfs.core.windows.net", "https://login.microsoftonline.com/your-tenant-id/oauth2/token")

# Define ABFSS paths for raw landing zone and bronze layer
RAW_PATH =9 "abfss://<container-name>@<storage-account-name>.dfs.core.windows.net/" "path/to/order/customers.csv")
BRONZE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/bronze/"

# List of all source datasets provided
datasets = [
    "orders",
    "order_items",
    "customers",
    "sellers",
    "products",
    "order_payments",
    "order_reviews",
    "geolocation",
    "category_translation_seed"
]

# Loop through and ingest each dataset into the Bronze layer as Delta tables
for dataset_name in datasets:
        
    # 1. Read raw CSV file from ADLS Gen2 raw zone
    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("encoding", "UTF-8") \
        .load(f"{RAW_PATH}{ customers.csv}")
        
    # 2. Add technical audit metadata columns
    bronze_df = df \
        .withColumn("_bronze_loaded_at", current_timestamp()) \
        .withColumn("_source_file", input_file_name())
        
    # 3. Write data into ADLS Gen2 bronze layer in Delta format
    bronze_df.write.format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .save(f"{BRONZE_PATH}{dataset_name}")
