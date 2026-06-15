from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, when

spark = SparkSession.builder.appName("MovieAnalysis").getOrCreate()

print("=" * 60)
print("A-1 Data Cleaning")
print("=" * 60)

# 读取 CSV
df = spark.read.option("header", "true") \
    .option("sep", ",") \
    .option("multiLine", "true") \
    .option("quote", "\"") \
    .csv("s3a://douban/douban_movies.csv")

# 过滤无效行
df = df.filter(col("movie_id").isNotNull())

print("\n1. Schema:")
df.printSchema()

print("\n2. First 5 rows:")
df.show(5)

print("\n3. Missing value ratio:")
total = df.count()
for c in df.columns:
    null_cnt = df.filter(col(c).isNull()).count()
    print(f"   {c}: {null_cnt}/{total} ({null_cnt/total*100:.1f}%)")

# 转换类型
df = df.withColumn("rating_score", col("rating_score").cast("double"))
df = df.withColumn("year", col("year").cast("int"))

# 数据清洗
print("4. Data cleaning:")
print("   Strategy 1: Keep rows with rating_score between 1 and 10")
df_clean = df.filter((col("rating_score") >= 1) & (col("rating_score") <= 10))

print("   Strategy 2: Fill missing genres with 'Unknown' and replace '[]' with 'Unknown'")
df_clean = df_clean.fillna({"genres": "Unknown"})
df_clean = df_clean.withColumn(
    "genres",
    when(col("genres") == "[]", "Unknown").otherwise(col("genres"))
)

# 类型转换改进
numeric_cols = ["year", "rating_score", "rating_count", "collect_count"]
for c in numeric_cols:
    df_clean = df_clean.withColumn(c, col(c).cast("double"))

df_numeric = df_clean.select(*numeric_cols)

print("\n6. Statistics for numeric fields (mean, stddev, min, max):")
df_numeric.describe().show()

# ========== A-2 Spark SQL Analysis ==========
print("\n" + "=" * 60)
print("A-2 Spark SQL Analysis")
print("=" * 60)

# 拆分 genres（用于查询1和查询4）
df_exploded = df_clean.withColumn("genre", explode(split(col("genres"), "/")))
df_exploded.createOrReplaceTempView("movies_exploded")

# 原始表（用于查询2和查询3）
df_clean.createOrReplaceTempView("movies")

# Query 1: GROUP BY
print("\nQuery 1: Average rating by genre")
spark.sql("""
    SELECT genre, COUNT(*) as count, ROUND(AVG(rating_score), 2) as avg_rating
    FROM movies_exploded
    WHERE rating_score IS NOT NULL AND genre IS NOT NULL AND genre != ''
    GROUP BY genre
    ORDER BY avg_rating DESC
    LIMIT 10
""").show(truncate=False)

# Query 2: Top-N
print("\nQuery 2: Top 10 highest rated movies")
spark.sql("""
    SELECT DISTINCT title, rating_score, rating_count, year
    FROM movies
    WHERE rating_score IS NOT NULL
    ORDER BY rating_score DESC, rating_count DESC
    LIMIT 10
""").show(truncate=False)

# Query 3: Time dimension
print("\nQuery 3: Average rating trend by year")
spark.sql("""
    SELECT year, COUNT(*) as count, ROUND(AVG(rating_score), 2) as avg_rating
    FROM movies
    WHERE year IS NOT NULL AND rating_score IS NOT NULL AND year BETWEEN 1950 AND 2020
    GROUP BY year
    ORDER BY year
    LIMIT 30
""").show(truncate=False)

# Query 4: Top 1 movie per genre (window function)
print("\nQuery 4: Highest rated movie per genre")
spark.sql("""
    SELECT genre, title, rating_score
    FROM (
        SELECT genre, title, rating_score,
            ROW_NUMBER() OVER (PARTITION BY genre ORDER BY rating_score DESC) as rank
        FROM movies_exploded
        WHERE rating_score IS NOT NULL AND genre IS NOT NULL AND genre != ''
    ) t
    WHERE rank = 1
""").show(30, truncate=False)

print("\n" + "=" * 60)
print("Analysis completed!")
spark.stop()
