import pyspark.pandas as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id

from dotenv import load_dotenv
import os

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

# ============================================
# STEP 1: Spark Session + DB Connection Setup
# ============================================

spark = (
    SparkSession.builder
        .appName("ETL Pipeline")
        .config("spark.sql.ansi.enabled", "false")
        .config("spark.driver.extraJavaOptions", "-Djava.security.manager=allow")
        .config("spark.executor.extraJavaOptions", "-Djava.security.manager=allow")
        # Performance optimizations
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "4")  # Reduce for local mode
        .config("spark.sql.adaptive.enabled", "true")  # Adaptive query execution
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")

        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .getOrCreate()
)

# Load environment variables from .env file
load_dotenv()


jdbc_url = (
    f"jdbc:postgresql://{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/"
    f"{os.getenv('DB_NAME')}"
    "?sslmode=require"
    "&prepareThreshold=0"
    "&reWriteBatchedInserts=true"  # better batch performance
)

connection_props = {
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "driver": "org.postgresql.Driver"
}

# ============================================
# STEP 2: Extract Data From CSV File
# ============================================

df = ps.read_csv("dataset/vgchartz-2024.csv")


# Cache the main dataframe for reuse
df_spark = df.to_spark().cache()

# Add all IDs to the main dataframe first (before splitting into tables)
df_spark = df_spark.withColumn("sales_id", monotonically_increasing_id())
df_spark = df_spark.withColumn("creator_id", monotonically_increasing_id())
df_spark = df_spark.withColumn("games_id", monotonically_increasing_id())

df = df_spark.pandas_api()

# ============================================
# STEP 3: Sales Table
# ============================================

# Select sales columns (sales_id already exists in df)
sales_df = df[["sales_id", "na_sales", "jp_sales", "pal_sales", "other_sales", "total_sales"]]

# filling NaN with 0
sales_df = sales_df.fillna({
    "na_sales": 0,
    "jp_sales": 0,
    "pal_sales": 0,
    "other_sales": 0,
    "total_sales": 0
})

#debug print
#print("Sales Table:")
#print(sales_df.head(4))

# ============================================
# STEP 4: Creator Table
# ============================================

# Select creator columns (creator_id already exists in df)
creator_df = df[["creator_id", "developer", "publisher"]]

#debug print
#print("Creator Table:")
#print(creator_df.head(4))

# ============================================
# STEP 5: Games Table
# ============================================

# Select games columns (games_id, sales_id, creator_id already exist in df)
games_df = df[["games_id", "sales_id", "creator_id", "title", "genre", "console", "critic_score"]]

# filling NaN with 0 for critic_score
games_df["critic_score"] = games_df["critic_score"].fillna(0)

#debug print
#print("Games Table:")
#print(games_df.head(4))

# ============================================
# STEP 6: Convert to Spark DataFrames
# ============================================

sales_spark_df   = sales_df.to_spark()
creator_spark_df = creator_df.to_spark()
games_spark_df   = games_df.to_spark()

# ============================================
# STEP 7: Write to PostgreSQL Database
# ============================================

# sales table
sales_spark_df.write.jdbc(
    url=jdbc_url,
    table="public.Sales",
    mode="overwrite",          # overwrite for testing purposes
    properties=connection_props
)

# creator table
creator_spark_df.write.jdbc(
    url=jdbc_url,
    table="public.Creator",
    mode="overwrite",   # overwrite for testing purposes
    properties=connection_props
)

# games table
games_spark_df.write.jdbc(
    url=jdbc_url,
    table="public.Games",
    mode="overwrite",   # overwrite for testing purposes
    properties=connection_props
)

print("-----Pipeline Write Complete-----")