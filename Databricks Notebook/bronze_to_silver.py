# Databricks notebook source
# DBTITLE 1,Mounting the ADLS
# MAGIC %run "../mounting"
# MAGIC mounting_adls('ombronzedatalake', 'raw')
# MAGIC mounting_adls('silverdatalake', 'cleaned')
# MAGIC mounting_adls('ombronzedatalake', 'usernamemapping')
# MAGIC display(dbutils.fs.mounts())

# COMMAND ----------

# DBTITLE 1,Display Mounts
display(dbutils.fs.mounts())

# COMMAND ----------

# DBTITLE 1,Read the CSV and JSON file
# Read CSV to parquet converted file 
df_csv = spark.read.parquet("dbfs:/mnt/ombronzdatalake/raw/ecommerse_csv_dataset.parquet")

# Read JSON to parquet converted file 
df_json = spark.read.parquet("dbfs:/mnt/ombronzdatalake/raw/ecommerse_json_dataset.parquet")

# COMMAND ----------

# Read ecommerce JSON dataset (Parquet)
df_json = spark.read.parquet(
    "/Volumes/adh_hhd_dev/comment_generation/comment_scripts/A153902/ecommerce_json_dataset.parquet"
)

# Read ecommerce CSV dataset (Parquet)
df_csv = spark.read.parquet(
    "/Volumes/adh_hhd_dev/comment_generation/comment_scripts/A153902/ecommerce_csv_dataset.parquet"
)


# COMMAND ----------

# DBTITLE 1,Display CSV file data
display(df_csv)
display(df_csv.count())

# COMMAND ----------

# DBTITLE 1,Display JSON file data
display(df_json)
display(df_json.count())

# COMMAND ----------

# DBTITLE 1,Print Schema
df_csv.printSchema()
df_json.printSchema()

# COMMAND ----------

# DBTITLE 1,Cast Datatype
from pyspark.sql.functions import col

columns_to_cast = [
    "UserID", "Age", "ProductID", "Price", "Quantity", "TotalAmount",
    "HasDiscountApplied", "DiscountRate", "ReviewScore", "SessionDuration"
]

df_csv = df_csv.select([
    col(c).cast("string") if c in columns_to_cast else col(c)
    for c in df_csv.columns
])

df_json_aligned = df_json.select([
    col(c).cast("string") if c in columns_to_cast else col(c)
    for c in df_json.columns
])

# COMMAND ----------

# DBTITLE 1,Print Schema changed
df_csv.printSchema()
df_json_aligned.printSchema()

# COMMAND ----------

# DBTITLE 1,Combine the data by name
df_combined = df_csv.unionByName(df_json_aligned)
df = df_combined

# COMMAND ----------

# DBTITLE 1,Display combined df count
display(df.toPandas())
print(df.toPandas().shape[0])

# COMMAND ----------

# DBTITLE 1,Cast datatypes of columns to required datatype
from pyspark.sql.functions import (
    col,
    year,
    month,
    current_timestamp,
    input_file_name,
    regexp_replace,
    when,
    to_date
)

from pyspark.sql.types import DoubleType

# Fix LastLogin format
df_silver = df.withColumn(
    "LastLoginFormatted",
    regexp_replace(col("LastLogin"), r"\.(\d+)$", r":\1")
)

# Parse custom date format
df_parsed = (
    df_silver
        .withColumn("SignUpDate", to_date(col("SignUpDate"), "MM/dd/yyyy"))
        .withColumn("PurchaseDate", to_date(col("PurchaseDate"), "MM/dd/yyyy"))
)

# Derive year and month
df_enriched = (
    df_parsed
        .withColumn("SignupYear", year(col("SignUpDate")))
        .withColumn("PurchaseMonth", month(col("PurchaseDate")))
)


df_silver = (
    df_enriched
        .withColumn(
            "DiscountedAmount",
            col("TotalAmount").cast(DoubleType()) * col("DiscountRate").cast(DoubleType())
        )
        .withColumn(
            "FinalAmount",
            col("TotalAmount").cast(DoubleType()) - col("DiscountedAmount")
        )
)

# Add metadata
df_silver = (
    df_silver
        .withColumn("IngestionTimestamp", current_timestamp())
        # .withColumn("SourceFile", input_file_name())
)

# COMMAND ----------

display(df_silver)

# COMMAND ----------

# DBTITLE 1,Pseudonymize Data
# Pseudonymize userName

from pyspark.sql.functions import monotonically_increasing_id, rand, row_number
from pyspark.sql.window import Window

# Step 1: Add unique ID to original DataFrame
df_with_id = df_silver.withColumn("RowID", monotonically_increasing_id())

# Step 2: Create shuffled version of UserName with same RowIDs
usernames_shuffled = (
    df_with_id
        .select("RowID", "UserName")
        .orderBy(rand())
        .withColumn(
            "ShuffledUserName",
            row_number().over(Window.orderBy(rand()))
        )
        .drop("UserName")
)

# Step 3: Create mapping table
mapping_df = (
    df_with_id
        .select("RowID", "UserName")
        .join(usernames_shuffled, on="RowID", how="inner")
)

# Step 4: Replace UserName in original DataFrame
df_pseudonymized = (
    df_with_id
        .drop("UserName")
        .join(
            mapping_df.select("RowID", "ShuffledUserName"),
            on="RowID",
            how="inner"
        )
        .withColumnRenamed("ShuffledUserName", "UserName")
        .drop("RowID")
)

# COMMAND ----------

display(df_pseudonymized)

# COMMAND ----------

# DBTITLE 1,Write the silver table
# Write Silver data to Delta Lake
df_pseudonymized.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/mnt/silverdatalake/cleaned/user_purchase_silver")

# COMMAND ----------

# Create Delta table on top of the location
spark.sql("""
CREATE TABLE IF NOT EXISTS user_purchase_silver
USING DELTA
LOCATION 'dbfs:/mnt/silverdatalake/cleaned/user_purchase_silver'
""")
