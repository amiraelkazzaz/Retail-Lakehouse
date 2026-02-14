# retail_etl.py - Complete ETL Pipeline
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from minio import Minio
import pandas as pd
import io

print("=" * 70)
print("🚀 STARTING RETAIL ETL PIPELINE")
print("=" * 70)

# Step 1: Create Spark Session
print("\n[1/9] Creating Spark Session...")
spark = SparkSession.builder \
    .appName("Retail ETL") \
    .master("spark://spark-master:7077") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minio") \
    .config("spark.hadoop.fs.s3a.secret.key", "minio123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()
print("✅ Spark Session Created!")

# Step 2: Download Excel from MinIO
print("\n[2/9] Downloading Excel from MinIO...")
minio_client = Minio("minio:9000", access_key="minio", secret_key="minio123", secure=False)
response = minio_client.get_object("retails", "online retail ii.xlsx")
excel_data = response.read()
response.close()
response.release_conn()
print("✅ Excel Downloaded!")

# Step 3: Read Excel with Pandas
print("\n[3/9] Reading Excel sheets...")
excel_file = pd.ExcelFile(io.BytesIO(excel_data))
df_2009 = pd.read_excel(excel_file, sheet_name='Year 2009-2010')
df_2010 = pd.read_excel(excel_file, sheet_name='Year 2010-2011')
df_2009['fiscal_year'] = '2009-2010'
df_2010['fiscal_year'] = '2010-2011'
df_combined = pd.concat([df_2009, df_2010], ignore_index=True)
print(f"✅ Loaded {len(df_combined)} rows")

# Step 4: Convert to Spark
print("\n[4/9] Converting to Spark DataFrame...")
df_raw = spark.createDataFrame(df_combined)
print(f"✅ Spark DataFrame ready: {df_raw.count()} rows")

# Step 5: Clean Data
print("\n[5/9] Cleaning data...")
df_cleaned = df_raw \
    .dropna(subset=["Invoice", "StockCode", "Quantity", "Price"]) \
    .filter(col("Quantity") > 0) \
    .filter(col("Price") > 0) \
    .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"))) \
    .withColumn("TotalAmount", round(col("Quantity") * col("Price"), 2)) \
    .dropDuplicates(["Invoice", "StockCode", "InvoiceDate"])
print(f"✅ Cleaned: {df_cleaned.count()} rows")

# Step 6: Add Features
print("\n[6/9] Adding time-based features...")
df_enriched = df_cleaned \
    .withColumn("Year", year(col("InvoiceDate"))) \
    .withColumn("Month", month(col("InvoiceDate"))) \
    .withColumn("Quarter", quarter(col("InvoiceDate"))) \
    .withColumn("DayName", date_format(col("InvoiceDate"), "EEEE")) \
    .withColumn("Hour", hour(col("InvoiceDate"))) \
    .withColumn("MonthName", date_format(col("InvoiceDate"), "MMMM"))
print("✅ Features added!")

# Step 7: Create Analytics
print("\n[7/9] Creating analytics...")

# Customer RFM
max_date = df_cleaned.agg(max("InvoiceDate")).collect()[0][0]
customer_rfm = df_cleaned.groupBy("Customer ID") \
    .agg(
        max("InvoiceDate").alias("LastPurchaseDate"),
        count("Invoice").alias("Frequency"),
        sum("TotalAmount").alias("MonetaryValue")
    ) \
    .withColumn("Recency", datediff(lit(max_date), col("LastPurchaseDate")))
print("  ✓ Customer RFM")

# Product Performance
product_analysis = df_cleaned.groupBy("StockCode", "Description") \
    .agg(
        sum("Quantity").alias("TotalQuantitySold"),
        sum("TotalAmount").alias("TotalRevenue"),
        countDistinct("Customer ID").alias("UniqueCustomers")
    )
print("  ✓ Product Performance")

# Country Sales
country_sales = df_cleaned.groupBy("Country") \
    .agg(
        sum("TotalAmount").alias("TotalRevenue"),
        count("Invoice").alias("TotalOrders"),
        countDistinct("Customer ID").alias("UniqueCustomers")
    )
print("  ✓ Country Sales")

# Monthly Trends
monthly_trends = df_enriched.groupBy("fiscal_year", "Year", "Month", "MonthName") \
    .agg(
        sum("TotalAmount").alias("MonthlyRevenue"),
        count("Invoice").alias("MonthlyOrders")
    )
print("  ✓ Monthly Trends")

# Step 8: Save to MinIO
print("\n[8/9] Saving results to MinIO (Parquet format)...")

df_cleaned.write.mode("overwrite").partitionBy("fiscal_year").parquet("s3a://retails/processed/retail_cleaned")
print("  ✓ Saved: retail_cleaned")

df_enriched.write.mode("overwrite").partitionBy("fiscal_year", "Month").parquet("s3a://retails/processed/retail_enriched")
print("  ✓ Saved: retail_enriched")

customer_rfm.write.mode("overwrite").parquet("s3a://retails/analytics/customer_rfm")
print("  ✓ Saved: customer_rfm")

product_analysis.write.mode("overwrite").parquet("s3a://retails/analytics/product_performance")
print("  ✓ Saved: product_performance")

country_sales.write.mode("overwrite").parquet("s3a://retails/analytics/country_sales")
print("  ✓ Saved: country_sales")

monthly_trends.write.mode("overwrite").parquet("s3a://retails/analytics/monthly_trends")
print("  ✓ Saved: monthly_trends")

# Step 9: Summary
print("\n[9/9] Generating summary...")
total_revenue = df_cleaned.agg(sum("TotalAmount")).collect()[0][0]
total_orders = df_cleaned.select("Invoice").distinct().count()
unique_customers = df_cleaned.select("Customer ID").distinct().count()
unique_products = df_cleaned.select("StockCode").distinct().count()

print("\n" + "=" * 70)
print("📊 BUSINESS SUMMARY")
print("=" * 70)
print(f"💰 Total Revenue:     ${total_revenue:,.2f}")
print(f"📦 Total Orders:      {total_orders:,}")
print(f"👥 Unique Customers:  {unique_customers:,}")
print(f"🏷️  Unique Products:   {unique_products:,}")
print("=" * 70)
print("\n🎉 ETL PIPELINE COMPLETED SUCCESSFULLY!")
print("=" * 70)

spark.stop()