
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, month, year, sum, countDistinct


spark = SparkSession.builder.appName("Retail Transactions Analysis").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("data/retail_transactions.csv")

df_cleaned = df.filter(
    (col("InvoiceDate") != "0000-00-00 00:00:00") &
    (col("UnitPrice") > 0) &
    (col("Quantity") > 0)
)

#create a temporary view for SQL queries
df_cleaned.createOrReplaceTempView("retail")

#  summarize Totals using sql
Totals = spark.sql("""
    SELECT 
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT InvoiceNo) AS unique_invoices,
        SUM(Quantity * UnitPrice) AS total_sales,
        COUNT(DISTINCT CustomerID) AS unique_customers,
        COUNT(DISTINCT Country) AS countries_involved
    FROM retail
""")


Totals.show()


# trend analysis
sales = df_cleaned.withColumn("Date", to_date("InvoiceDate"))                    .withColumn("Month", month("Date"))                    .withColumn("Year", year("Date"))

# sales monthly trend
monthly_sales = sales.groupBy("Year", "Month").agg(
    sum(col("Quantity") * col("UnitPrice")).alias("MonthlySales")
)
monthly_sales.coalesce(1).write.option("header", True).csv("output/monthly_sales")

# top products analysis
top_products = sales.groupBy("Description").agg(
    sum("Quantity").alias("TotalSold")
).orderBy(col("TotalSold").desc())
top_products.coalesce(1).write.option("header", True).csv("output/top_products")

# customer behavior analysis
customer_behavior = sales.groupBy("CustomerID").agg(
    countDistinct("InvoiceNo").alias("Purchases"),
    sum(col("Quantity") * col("UnitPrice")).alias("TotalSpent")
)
customer_behavior.coalesce(1).write.option("header", True).csv("output/customer_behavior")

# 4. countries sales volume
sales_by_country = sales.groupBy("Country").agg(
    sum(col("Quantity") * col("UnitPrice")).alias("TotalSales")
).orderBy(col("TotalSales").desc())
sales_by_country.coalesce(1).write.option("header", True).csv("output/sales_by_country")

print("Analysis complete. Results saved in the output directory.")
