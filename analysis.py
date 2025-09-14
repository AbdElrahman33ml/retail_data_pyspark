
from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = SparkSession.builder.appName("Retail Transactions Analysis").getOrCreate()

df = spark.read.option("header", True).option("inferSchema", True).csv("data/retail_transactions.csv")

df_cleaned = df.filter(
    (col("InvoiceDate") != "0000-00-00 00:00:00") &
    (col("UnitPrice") > 0) &
    (col("Quantity") > 0)
)

#create a temporary view for SQL queries
df_cleaned.createOrReplaceTempView("retail")

#  summarize statistics using sql
statistics = spark.sql("""
    SELECT 
        COUNT(*) AS total_transactions,
        COUNT(DISTINCT InvoiceNo) AS unique_invoices,
        SUM(Quantity * UnitPrice) AS total_sales,
        COUNT(DISTINCT CustomerID) AS unique_customers,
        COUNT(DISTINCT Country) AS countries_involved
    FROM retail
""")


statistics.show()
