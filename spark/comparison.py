import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, when, count, avg

DATA_PATH = "s3a://douban/douban_movies.csv"
RUNS = 3

def load_data_spark(spark):
    """加载并清洗数据"""
    df = spark.read.option("header", "true") \
        .option("sep", ",") \
        .option("multiLine", "true") \
        .option("quote", "\"") \
        .csv(DATA_PATH)
    
    # 过滤空 movie_id
    df = df.filter(col("movie_id").isNotNull())
    
    # 转换评分为 double 类型
    df = df.withColumn("rating_score", col("rating_score").cast("double"))
    
    # 处理缺失的类型字段
    df = df.fillna({"genres": "Unknown"})
    df = df.withColumn("genres",
        when(col("genres") == "[]", "Unknown").otherwise(col("genres")))
    
    return df

def query1_pyspark(df):
    """查询1: 按电影类型统计数量和平均评分"""
    # 展开类型（按"/"分割）
    df_exploded = df.withColumn("genre", explode(split(col("genres"), "/")))
    
    # 过滤无效数据
    df_exploded = df_exploded.filter(
        (col("rating_score").isNotNull()) &
        (col("genre").isNotNull()) &
        (col("genre") != ""))
    
    # 按类型分组统计
    result = df_exploded.groupBy("genre").agg(
        count("*").alias("count"),
        avg("rating_score").alias("avg_rating"))
    
    # 按平均评分降序排序，取前10
    result = result.orderBy(col("avg_rating").desc()).limit(10)
    
    return result

def create_spark_session(executor_instances=1):
    # 获取当前 driver 的 service 名称
    # 在 Kubernetes 中，driver service 通常命名为 <driver-pod-name>-svc
    
    builder = SparkSession.builder \
        .appName(f"Query1_Perf_e{executor_instances}") \
        .config("spark.executor.instances", str(executor_instances)) \
        .config("spark.executor.cores", "1") \
        .config("spark.executor.memory", "1g") \
        .config("spark.driver.memory", "1g") \
        .config("spark.sql.shuffle.partitions", str(executor_instances * 2)) \
        .config("spark.kubernetes.driver.service.name", f"pyspark-analysis-driver-svc") \
        .config("spark.kubernetes.driver.host", f"pyspark-analysis-driver-svc.default.svc") \
        .config("spark.driver.host", f"pyspark-analysis-driver-svc.default.svc") \
        .config("spark.driver.port", "7078") \
        .config("spark.driver.blockManager.port", "7079") \
        .config("spark.kubernetes.driver.pod.name", "pyspark-analysis-driver") \
        .config("spark.kubernetes.namespace", "default") \
        .config("spark.kubernetes.container.image", "apache/spark:3.4.3") \
        .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark") \
        .config("spark.kubernetes.executor.deleteOnTermination", "true")
    
    return builder.getOrCreate()

def run_benchmark():
    """运行性能基准测试"""
    print("=" * 60)
    print("Query 1 Performance Benchmark: PySpark Only")
    print("=" * 60)
    
    times = {"PySpark(x1)": [], "PySpark(x2)": []}
    
    for executor_instances in [1, 2]:
        spark = create_spark_session(executor_instances)
        
        print(f"\n测试 PySpark(x{executor_instances})...")
        
        # 加载数据并缓存
        df = load_data_spark(spark)
        df.cache()
        total_count = df.count()  # 触发缓存
        print(f"加载完成，共 {total_count} 行")
        
        # 预热查询（避免第一次运行偏慢）
        _ = query1_pyspark(df).collect()
        
        # 正式测试
        key = f"PySpark(x{executor_instances})"
        for i in range(RUNS):
            start = time.time()
            result = query1_pyspark(df)
            result.collect()
            times[key].append(time.time() - start)
            print(f"  运行 {i+1}/{RUNS}: {times[key][-1]:.3f}秒")
        
        spark.stop()
    
    # 计算平均时间
    avg_times = {k: sum(v) / len(v) for k, v in times.items()}
    
    # 输出结果
    print("\n" + "=" * 60)
    print("最终结果:")
    print("=" * 60)
    print(f"PySpark(x1) 平均时间: {avg_times['PySpark(x1)']:.4f}秒")
    print(f"PySpark(x2) 平均时间: {avg_times['PySpark(x2)']:.4f}秒")
    
    speedup = avg_times['PySpark(x1)'] / avg_times['PySpark(x2)']
    print(f"加速比: {speedup:.2f}x")
    
    print("\nFINAL_RESULTS:")
    print(f"PYSPARK_X1_TIME={avg_times['PySpark(x1)']:.4f}")
    print(f"PYSPARK_X2_TIME={avg_times['PySpark(x2)']:.4f}")
    print("=" * 60)

if __name__ == "__main__":
    run_benchmark()
