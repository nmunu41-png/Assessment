def run_data_quality_tests(spark, gold_path):
    print("Running Data Quality Checks...")
    
    fact_items = spark.read.format("delta").load(f"{gold_path}fact_order_item")
    dim_cust = spark.read.format("delta").load(f"{gold_path}dim_customer")
    dim_prod = spark.read.format("delta").load(f"{gold_path}dim_product")
    
    # Test 1: Key Integrity - Non-null critical surrogate keys
    null_keys = fact_items.filter(col("customer_sk").isNull() | col("product_sk").isNull()).count()
    assert null_keys == 0, f"DQ FAIL: Found {null_keys} rows with null surrogate keys in fact_order_item."

    # Test 2: Referential Integrity - Fact keys must exist in dimensions
    orphan_cust = fact_items.join(dim_cust, fact_items.customer_sk == dim_cust.customer_sk, "left_anti") \
        .filter(col("customer_sk") != -1).count()
    assert orphan_cust == 0, f"DQ FAIL: {orphan_cust} orphan records found against dim_customer."

    # Test 3: Temporal Sanity - Price cannot be negative
    negative_price = fact_items.filter(col("item_price") < 0).count()
    assert negative_price == 0, f"DQ FAIL: {negative_price} items found with negative pricing."

    # Test 4: Duplicate Detection at Fact Grain (order_id + order_item_id)
    total_rows = fact_items.count()
    distinct_rows = fact_items.dropDuplicates(["order_id", "order_item_id"]).count()
    assert total_rows == distinct_rows, f"DQ FAIL: Duplicate entries detected at the fact grain! Total: {total_rows}, Distinct: {distinct_rows}"

    # Test 5: Format & Content - Review score bounds (if joined/validated)
    # Checked via business logic rule: Item value must be greater than zero
    invalid_value = fact_items.filter(col("total_item_value") <= 0).count()
    assert invalid_value == 0, f"DQ FAIL: {invalid_value} items have non-positive total value."

    # Test 6: Freshness Check - Ensure data contains records up to expected timeframe
    max_date = fact_items.selectExpr("max(order_year)").collect()[0][0]
    assert max_date >= 2016, f"DQ FAIL: Freshness/Timeframe violation. Max year found is {max_date}."

    # Test 7: Referential Integrity for Product Dimension
    orphan_prod = fact_items.join(dim_prod, fact_items.product_sk == dim_prod.product_sk, "left_anti") \
        .filter(col("product_sk") != -1).count()
    assert orphan_prod == 0, f"DQ FAIL: {orphan_prod} orphan records found against dim_product."

    # Test 8: Business Rule Check - Freight cost cannot exceed 5x item price (Anomaly guardrail)
    excessive_freight = fact_items.filter(col("freight_cost") > (col("item_price") * 5)).count()
    assert excessive_freight < (total_rows * 0.01), f"DQ FAIL: Excessive freight anomaly rate exceeded threshold ({excessive_freight} rows)."

    print("All 8 Data Quality checks passed successfully!")
