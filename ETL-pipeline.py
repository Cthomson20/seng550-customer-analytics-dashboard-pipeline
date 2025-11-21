import pyspark.pandas as ps
from pyspark.sql import SparkSession

# ============================================
# STEP 1: Spark Session
# ============================================

spark = (
    SparkSession.builder
        .appName("ETL Pipeline")
        .config("spark.sql.ansi.enabled", "false")   # IMPORTANT FIX
        .getOrCreate()
)

ps.set_option("compute.fail_on_ansi_mode", False)

# ============================================
# STEP 2: Extract Data From CSV File
# ============================================

df = ps.read_csv("dataset/vgchartz-2024.csv", index_col=0)

# ============================================
# STEP 3: Sales Table
# ============================================

# copy sales columns
sales_df = df[["na_sales", "jp_sales", "pal_sales", "other_sales", "total_sales"]].copy()

# create sales_id
sales_df["sales_id"] = ps.arrange(len(sales_df))

# add sales_id to the front column
sales_df = sales_df[["sales_id", "na_sales", "jp_sales", "pal_sales", "other_sales", "total_sales"]]

print("Sales Table:")
print(sales_df.head(2))


# Transform
# table two: Sales - sales_id (PK), total_sales, jp_sales, pal_sales, other_sales, na_sales
# table three: Creator - creator_id (PK), developer, publisher
# table one: Games - games_id (PK), sales_id (FK), creator_id (FK), title, genre, console, critic_score

print("Extracted Data:")

# Load into Supabase