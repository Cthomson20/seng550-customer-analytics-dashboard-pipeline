import pyspark.pandas as ps
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ETL Pipeline") \
    .getOrCreate()

# Extract
df = ps.read_csv("vgchartz-2024.csv", header=True, inferSchema=True)

# Transform
# table one: Games - games_id (PK), sales_id (FK), creator_id (FK), title, genre, console, critic_score
# table two: Sales - sales_id (PK), total_sales, jp_sales (Japan), pal_sales (), other_sales, na_sales
# table three: Creator - creator_id (PK), developer, publisher


# Load into Supabase