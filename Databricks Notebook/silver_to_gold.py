# Databricks notebook source
# MAGIC %run "..mounting"

# COMMAND ----------

# DBTITLE 1,Mount Gold data lake
mount_adls("golddatalake", "analytics")

# COMMAND ----------

# DBTITLE 1,Read the file
df_gold = spark.read.format("delta").load("/mnt/silverdatalake/cleaned/user_purchase_silver")

# COMMAND ----------

display(df_gold)

# COMMAND ----------

# DBTITLE 1,Aggregations
from pyspark.sql.functions import (
    col,
    to_timestamp,
    to_date,
    count,
    sum,
    avg,
    when,
    current_timestamp,
    date_format
)
from pyspark.sql.types import DoubleType, IntegerType, BooleanType

# Country-level aggregations
df_gold.groupBy("Country").agg(
    count("*").alias("TotalTransactions"),
    sum("FinalAmount").alias("TotalSpend"),
    avg("ReviewScore").alias("AvgReviewScore")
)

# Category-level aggregations
df_gold.groupBy("Category").agg(
    count("*").alias("UnitsSold"),
    sum("TotalAmount").alias("Revenue"),
    avg("DiscountRate").alias("AvgDiscount")
)

# Device-level aggregations
df_gold.groupBy("DeviceType").agg(
    avg("SessionDuration").alias("AvgSessionTime"),
    avg("ReviewScore").alias("AvgSatisfaction")
)

# Signup year aggregations
df_gold.groupBy("SignupYear").agg(
    count("*").alias("UsersJoined"),
    avg("FinalAmount").alias("AvgSpendPerUser")
)

from pyspark.sql.functions import when, col, count, avg, current_timestamp, date_format

# Create ReviewTier column
df_gold = df_gold.withColumn(
    "ReviewTier",
    when(col("ReviewScore") > 4.5, "Excellent")
    .when(col("ReviewScore") > 3.5, "Good")
    .otherwise("Needs Improvement")
)

# Aggregation by ReviewTier
df_gold.groupBy("ReviewTier").agg(
    count("*").alias("Transactions"),
    avg("FinalAmount").alias("AvgSpend")
)

# Add Gold-layer metadata
df_gold = (
    df_gold
        .withColumn("GoldIngestionTimestamp", current_timestamp())
        .withColumn("GoldBatchID", date_format(current_timestamp(), "yyyy-MM-dd"))
)

# COMMAND ----------

# DBTITLE 1,Final Dataframe
from pyspark.sql.functions import col, to_timestamp, to_date
from pyspark.sql.types import DoubleType, IntegerType, BooleanType

df_gold = (
    df_gold
        .withColumn("Age", col("Age").cast(IntegerType()))
        .withColumn("Price", col("Price").cast(DoubleType()))
        .withColumn("Quantity", col("Quantity").cast(IntegerType()))
        .withColumn("TotalAmount", col("TotalAmount").cast(DoubleType()))
        .withColumn("DiscountRate", col("DiscountRate").cast(DoubleType()))
        .withColumn("ReviewScore", col("ReviewScore").cast(DoubleType()))
        .withColumn("SessionDuration", col("SessionDuration").cast(DoubleType()))
        .withColumn(
            "HasDiscountApplied",
            col("HasDiscountApplied").cast(BooleanType())
        )
        .withColumn(
            "LastLogin",
            to_timestamp(col("LastLogin"), "HH:mm:ss")
        )
        .withColumn(
            "SignUpDate",
            to_date(col("SignUpDate"))
        )
        .withColumn(
            "PurchaseDate",
            to_date(col("PurchaseDate"))
        )
        .withColumn(
            "IngestionTimestamp",
            col("IngestionTimestamp").cast("timestamp")
        )
        .withColumn(
            "GoldIngestionTimestamp",
            col("GoldIngestionTimestamp").cast("timestamp")
        )
        .withColumn(
            "UserName",
            col("UserName").cast(IntegerType())
        )
        .withColumn(
            "SignupYear",
            col("SignupYear").cast(IntegerType())
        )
        .withColumn(
            "PurchaseMonth",
            col("PurchaseMonth").cast(IntegerType())
        )
        .withColumn(
            "GoldBatchID",
            col("GoldBatchID").cast("string")
        )
)

# COMMAND ----------

display(df_gold)
df_gold.printSchema()

# COMMAND ----------

# DBTITLE 1,Write the Gold table
# Write Gold data to Delta Lake
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .save("dbfs:/mnt/golddatalake/analytics/user_purchases_gold")


# COMMAND ----------

# Create Gold Delta table
spark.sql("""
CREATE TABLE IF NOT EXISTS user_purchases_gold
USING DELTA
LOCATION 'dbfs:/mnt/golddatalake/analytics/user_purchases_gold'
""")