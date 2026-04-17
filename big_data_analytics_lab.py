from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# JVM flags for Java 17+ support
jvm_flags = (
    "--add-opens=java.base/java.lang=ALL-UNNAMED "
    "--add-opens=java.base/java.lang.invoke=ALL-UNNAMED "
    "--add-opens=java.base/java.io=ALL-UNNAMED "
    "--add-opens=java.base/java.net=ALL-UNNAMED "
    "--add-opens=java.base/java.nio=ALL-UNNAMED "
    "--add-opens=java.base/java.util=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent=ALL-UNNAMED "
    "--add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED "
    "--add-opens=java.base/sun.nio.cs=ALL-UNNAMED "
    "--add-opens=java.base/sun.security.action=ALL-UNNAMED "
    "--add-opens=java.base/sun.util.calendar=ALL-UNNAMED "
    "--add-opens=java.base/javax.security.auth=ALL-UNNAMED "
    "-Djava.security.manager=allow"
)

# Initialize Spark Session
spark = SparkSession.builder \
    .appName("BigDataAnalyticsLab") \
    .master("local[1]") \
    .config("spark.driver.extraJavaOptions", jvm_flags) \
    .config("spark.executor.extraJavaOptions", jvm_flags) \
    .config("spark.sql.session.timeZone", "UTC") \
    .config("spark.sql.shuffle.partitions", "1") \
    .config("spark.ui.enabled", "false") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# 0. DATASET & SCHEMA (from README)
transactions_data = [
    (1,  "T001", "Alice",   "North", "Electronics", 899.99, 2, "2024-01-05 10:30:00", "credit_card"),
    (2,  "T002", "Bob",     "South", "Clothing",     45.00, 3, "2024-01-06 11:00:00", "cash"),
    (3,  "T003", "Charlie", "East",  "Electronics", 199.50, 1, "2024-01-06 14:20:00", "debit_card"),
    (4,  "T004", "Alice",   "North", "Food",          12.50, 5, "2024-01-07 09:15:00", "cash"),
    (5,  "T005", "David",   "West",  "Electronics", 450.00, 1, "2024-01-08 16:45:00", "credit_card"),
    (6,  "T006", "Eve",     "South", "Food",          22.00, 4, "2024-01-08 18:00:00", "credit_card"),
    (7,  "T007", "Frank",   "North", "Clothing",     75.00, 2, "2024-01-09 13:30:00", "debit_card"),
    (8,  "T008", "Grace",   "East",  "Food",          33.00, 3, "2024-01-10 10:00:00", "cash"),
    (9,  "T009", "Heidi",   "West",  "Electronics", 600.00, 1, "2024-02-01 12:00:00", "credit_card"),
    (10, "T010", "Ivan",    "South", "Clothing",    110.00, 2, "2024-02-02 15:30:00", "debit_card"),
    (11, "T011", "Alice",   "North", "Electronics", 250.00, 1, "2024-02-03 09:00:00", "credit_card"),
    (12, "T012", "Bob",     "South", "Food",         18.00, 6, "2024-02-04 17:00:00", "cash"),
    (13, "T013", "Charlie", "East",  "Clothing",     95.00, 1, "2024-02-05 11:45:00", "credit_card"),
    (14, "T014", "David",   "West",  "Food",          8.50, 2, "2024-02-06 08:30:00", "debit_card"),
    (15, "T015", "Eve",     "South", "Electronics", 320.00, 1, "2024-02-07 14:00:00", "credit_card"),
    (16, "T016", "Frank",   "North", "Food",         55.00, 3, "2024-03-01 10:15:00", "cash"),
    (17, "T017", "Grace",   "East",  "Electronics", 780.00, 2, "2024-03-02 16:00:00", "credit_card"),
    (18, "T018", "Heidi",   "West",  "Clothing",    200.00, 1, "2024-03-03 12:30:00", "debit_card"),
    (19, "T019", "Ivan",    "South", "Food",         40.00, 5, "2024-03-04 09:45:00", "cash"),
    (20, "T020", "Alice",   "North", "Electronics", 999.99, 1, "2024-03-05 11:00:00", "credit_card"),
]

schema = StructType([
    StructField("id",             IntegerType(), True),
    StructField("transaction_id", StringType(),  True),
    StructField("customer",       StringType(),  True),
    StructField("region",         StringType(),  True),
    StructField("category",       StringType(),  True),
    StructField("unit_price",     DoubleType(),  True),
    StructField("quantity",       IntegerType(), True),
    StructField("timestamp",      StringType(),  True),
    StructField("payment_method", StringType(),  True),
])

df = spark.createDataFrame(transactions_data, schema)
df = df.withColumn("timestamp", F.to_timestamp("timestamp"))
df = df.withColumn("revenue", F.col("unit_price") * F.col("quantity"))

print("\n--- Part 1: Descriptive Analytics (Exercise 1 Integrated) ---")
# Descriptive Exercise 1: Add revenue_per_unit and find most expensive category per region
df = df.withColumn("revenue_per_unit", F.col("revenue") / F.col("quantity"))

region_window = Window.partitionBy("region").orderBy(F.desc("revenue_per_unit"))
df.withColumn("rank", F.row_number().over(region_window)) \
  .filter(F.col("rank") == 1) \
  .select("region", "category", "revenue_per_unit") \
  .show()

print("--- Part 2: Diagnostic Analytics (Monthly Trends via Pivot) ---")
df.withColumn("month", F.month("timestamp")) \
  .groupBy("month").pivot("region").sum("revenue").orderBy("month").show()

print("--- Part 3: Advanced Analytics (Rankings & Running Totals) ---")
customer_window = Window.partitionBy("customer").orderBy("timestamp")
df.withColumn("rank", F.rank().over(customer_window)) \
  .withColumn("running_total", F.sum("revenue").over(customer_window)) \
  .select("customer", "timestamp", "revenue", "rank", "running_total").show()

print("--- Part 4: Predictive Analytics (RFM Scoring) ---")
max_date = df.select(F.max("timestamp")).collect()[0][0]
rfm = df.groupBy("customer").agg(
    F.datediff(F.lit(max_date), F.max("timestamp")).alias("recency"),
    F.count("transaction_id").alias("frequency"),
    F.sum("revenue").alias("monetary")
)
# Scoring: 1 is best, 3 is worst (using ntile)
rfm_scored = rfm.withColumn("r_score", F.ntile(3).over(Window.orderBy("recency"))) \
                .withColumn("f_score", F.ntile(3).over(Window.orderBy(F.desc("frequency")))) \
                .withColumn("m_score", F.ntile(3).over(Window.orderBy(F.desc("monetary"))))
rfm_scored.show()

print("--- Part 5: Use Case (Customer Segmentation) ---")
rfm_scored.withColumn("segment", 
    F.when((F.col("r_score") == 1) & (F.col("m_score") == 1), "Champions")
     .when(F.col("r_score") == 3, "At Risk")
     .otherwise("Loyal Customers")
).select("customer", "segment").show()

print("--- Part 6: Use Case (Anomaly Detection via Z-Score) ---")
stats = df.select(F.mean("revenue").alias("avg"), F.stddev("revenue").alias("std")).collect()[0]
df_z = df.withColumn("z_score", (F.col("revenue") - stats['avg']) / stats['std'])
print("Transactions with Revenue Z-Score > 2.0 (Anomalies):")
df_z.filter(F.abs(F.col("z_score")) > 2.0).select("transaction_id", "revenue", "z_score").show()

print("--- Part 7: Data Engineering (Parquet Export) ---")
output_path = "retail_analytics_output.parquet"
df.write.mode("overwrite").parquet(output_path)
print(f"Data exported successfully to {output_path}")

print("\n--- Exercise 2: Credit Card vs Cash Average Revenue ---")
df.filter(F.col("payment_method").isin("credit_card", "cash")) \
  .groupBy("payment_method").agg(F.avg("revenue").alias("avg_revenue")) \
  .show()

print("--- Exercise 3: Previous Transaction Revenue per Customer (F.lag) ---")
df.withColumn("prev_transaction_revenue",
    F.lag("revenue").over(Window.partitionBy("customer").orderBy("timestamp"))) \
  .select("customer", "timestamp", "revenue", "prev_transaction_revenue").show()

print("--- Exercise 4: High Quantity Flag vs Payment Method ---")
df.withColumn("high_quantity", F.when(F.col("quantity") > 3, 1).otherwise(0)) \
  .groupBy("payment_method").agg(F.avg("high_quantity").alias("high_qty_rate")) \
  .show()

print("--- Exercise 5: RFM Segmentation with Adjusted Thresholds (ntile=4) ---")
rfm_adj = rfm.withColumn("r_score", F.ntile(4).over(Window.orderBy("recency"))) \
             .withColumn("f_score", F.ntile(4).over(Window.orderBy(F.desc("frequency")))) \
             .withColumn("m_score", F.ntile(4).over(Window.orderBy(F.desc("monetary"))))
rfm_adj.withColumn("segment",
    F.when((F.col("r_score") == 1) & (F.col("m_score") == 1), "Champions")
     .when(F.col("r_score") == 4, "At Risk")
     .otherwise("Loyal Customers")
).select("customer", "r_score", "f_score", "m_score", "segment").show()

print("--- Exercise 6: Anomaly Detection at 1.5 Sigma ---")
print("Transactions with Revenue Z-Score > 1.5 (Anomalies):")
df_z.filter(F.abs(F.col("z_score")) > 1.5).select("transaction_id", "revenue", "z_score").show()

print("--- Exercise 7: Region Health Score ---")
df.groupBy("region").agg(
    F.sum("revenue").alias("total_revenue"),
    F.avg("revenue").alias("avg_order_value"),
    F.count("transaction_id").alias("tx_count")
).withColumn("region_health_score",
    (F.col("total_revenue") / F.lit(1000)) +
    (F.col("avg_order_value") / F.lit(100)) +
    F.col("tx_count")
).select("region", "total_revenue", "avg_order_value", "tx_count", "region_health_score") \
 .orderBy(F.desc("region_health_score")).show()

spark.stop()