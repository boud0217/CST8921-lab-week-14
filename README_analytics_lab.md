# Big Data Analytics Lab
## Optimize Business Processes Using Big Data Analytics

---

### Overview
This lab walks through the full spectrum of big data analytics using **PySpark** on a
simulated retail transactions dataset. You will explore four types of analytics, apply
window functions, engineer ML features, and build two real-world use cases.

---

### Prerequisites
- Python 3.9+
- PySpark 4.x
- Java 17 (set `JAVA_HOME=/opt/homebrew/opt/openjdk@17`)
- `JAVA_TOOL_OPTIONS=--add-opens=java.base/javax.security.auth=ALL-UNNAMED`

### Run the Lab
```bash
python big_data_analytics_lab.py
```

---

### Lab Structure

| Part | Topic                        | Analytics Type  |
|------|------------------------------|-----------------|
| 1    | Summary stats, revenue by category/region | Descriptive |
| 2    | Drill-down, pivot, monthly trends         | Diagnostic  |
| 3    | Rankings, running totals, window functions | Advanced    |
| 4    | RFM scoring, ML feature engineering       | Predictive  |
| 5    | Customer segmentation (Champions → At Risk) | Use Case  |
| 6    | Anomaly detection using z-scores          | Use Case    |
| 7    | Parquet output for downstream use         | Data Engineering |

---

### Techniques Covered
- **Aggregations**: groupBy, pivot, describe
- **Window Functions**: rank(), ntile(), running totals, partitionBy
- **Feature Engineering**: hour/day/month extraction, RFM scoring, binary flags
- **Statistical Methods**: mean, stddev, z-score thresholds
- **SQL**: Spark SQL equivalents via DataFrame API

### Tools Used
- **PySpark DataFrame API** — distributed data processing
- **Spark SQL functions** — F.col, F.when, F.window, F.ntile, etc.
- **Parquet** — columnar storage for analytics outputs

---

### Hands-On Exercises

Try these modifications after running the lab:

1. **Descriptive**: Add a `revenue_per_unit` column and find the most expensive category per region.

2. **Diagnostic**: Filter to only `credit_card` transactions and compare their average revenue vs `cash`.

3. **Window Functions**: Add a `prev_transaction_revenue` column using `F.lag()` to see each customer's previous purchase.

4. **Feature Engineering**: Add a `high_quantity` flag (quantity > 3) and check if it correlates with payment method.

5. **Segmentation**: Adjust the RFM scoring thresholds and observe how segment sizes change.

6. **Anomaly Detection**: Change the threshold from 2σ to 1.5σ — how many more anomalies are flagged?

7. **Challenge**: Build a `region_health_score` that combines total revenue, avg order value, and transaction count into a single composite score per region.

# DATASET — Simulated retail transactions across regions
# ============================================================
transactions = [
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