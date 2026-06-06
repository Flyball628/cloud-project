"""
wordcount.py - PySpark 入门示例，用于验证 Spark on K8s 作业提交
"""
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("WordCount").getOrCreate()

# 读取示例文本（OBS 路径由教师提供）
# lines = spark.sparkContext.textFile("s3a://<BUCKET>/sample.txt")

# 本地测试用
lines = spark.sparkContext.parallelize([
    "hello world",
    "hello spark",
    "hello kubernetes",
    "spark on kubernetes",
    "world of big data"
])

word_counts = (
    lines.flatMap(lambda line: line.split())
         .map(lambda word: (word, 1))
         .reduceByKey(lambda a, b: a + b)
         .sortBy(lambda x: x[1], ascending=False)
)

print("Top 10 words:", word_counts.take(10))
spark.stop()
