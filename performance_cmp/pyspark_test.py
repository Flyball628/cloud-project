import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, explode, when, count, avg
from pyspark.sql.types import DoubleType

DATA_PATH = "./douban_movies.csv"
RUNS = 3


def load_data_spark(spark):
    """加载并清洗数据 - 正确处理多行CSV"""

    # 关键：正确配置多行CSV读取
    df = spark.read \
        .option("header", "true") \
        .option("sep", ",") \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .option("multiLine", "true") \
        .option("mode", "PERMISSIVE") \
        .csv(DATA_PATH)

    print("✓ 数据加载成功")
    print(f"  原始列数: {len(df.columns)}")

    # 检查是否有解析错误的行
    if "_corrupt_record" in df.columns:
        corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()
        if corrupt_count > 0:
            print(f"  警告: {corrupt_count} 行解析失败，已跳过")

    # 过滤空 movie_id
    df = df.filter(col("movie_id").isNotNull())

    # 安全转换评分：先转为字符串，处理空值，再转数字
    df = df.withColumn("rating_score_str", col("rating_score").cast("string"))

    # 处理空字符串和 null
    df = df.withColumn("rating_score_str",
                       when(col("rating_score_str") == "", None)
                       .otherwise(col("rating_score_str")))

    # 转换为 double
    df = df.withColumn("rating_score", col("rating_score_str").cast(DoubleType()))
    df = df.drop("rating_score_str")

    # 处理 genres 字段
    df = df.fillna({"genres": "Unknown"})
    df = df.withColumn("genres",
                       when(col("genres") == "[]", "Unknown")
                       .when(col("genres") == "", "Unknown")
                       .otherwise(col("genres")))

    # 过滤无效评分
    df = df.filter(col("rating_score").isNotNull())

    print(f"  有效数据行数: {df.count()}")

    return df


def query1_pyspark(df):
    """查询1: 按电影类型统计数量和平均评分"""
    # 展开类型（按"/"分割）
    df_exploded = df.withColumn("genre", explode(split(col("genres"), "/")))

    # 过滤无效数据
    df_exploded = df_exploded.filter(
        (col("rating_score").isNotNull()) &
        (col("genre").isNotNull()) &
        (col("genre") != "") &
        (col("genre") != "Unknown"))

    # 按类型分组统计
    result = df_exploded.groupBy("genre").agg(
        count("*").alias("count"),
        avg("rating_score").alias("avg_rating"))

    # 按平均评分降序排序，取前10
    result = result.orderBy(col("avg_rating").desc()).limit(10)

    return result


def create_spark_session(executor_instances=1):
    """创建 SparkSession"""
    builder = SparkSession.builder \
        .appName(f"Query1_Perf_e{executor_instances}") \
        .config("spark.executor.instances", str(executor_instances)) \
        .config("spark.executor.cores", "1") \
        .config("spark.sql.shuffle.partitions", str(executor_instances * 2)) \
        .config("spark.sql.adaptive.enabled", "false")

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

        try:
            # 加载数据并缓存
            df = load_data_spark(spark)
            df.cache()
            total_count = df.count()
            print(f"✓ 缓存完成，共 {total_count} 行")

            if total_count == 0:
                print("错误：没有有效数据")
                continue

            # 显示数据预览
            print("\n数据预览:")
            df.select("movie_id", "rating_score", "genres").show(5, truncate=50)

            # 预热查询
            print("\n预热查询...")
            _ = query1_pyspark(df).collect()

            # 正式测试
            key = f"PySpark(x{executor_instances})"
            print(f"\n运行 {RUNS} 次性能测试...")
            for i in range(RUNS):
                start = time.time()
                result = query1_pyspark(df)
                result.collect()
                elapsed = time.time() - start
                times[key].append(elapsed)
                print(f"  运行 {i + 1}/{RUNS}: {elapsed:.3f}秒")

            # 显示查询结果
            print("\n查询结果（平均评分最高的10种类型）:")
            result = query1_pyspark(df)
            result.show(10, truncate=False)

        except Exception as e:
            print(f"错误: {e}")
            import traceback
            traceback.print_exc()
        finally:
            spark.stop()

    # 计算平均时间
    avg_times = {}
    for k, v in times.items():
        if v:
            avg_times[k] = sum(v) / len(v)

    # 输出结果
    print("\n" + "=" * 60)
    print("最终结果:")
    print("=" * 60)
    for k, v in avg_times.items():
        print(f"{k} 平均时间: {v:.4f}秒")

    if "PySpark(x1)" in avg_times and "PySpark(x2)" in avg_times:
        speedup = avg_times["PySpark(x1)"] / avg_times["PySpark(x2)"]
        print(f"加速比: {speedup:.2f}x")

        print("\nFINAL_RESULTS:")
        print(f"PYSPARK_X1_TIME={avg_times['PySpark(x1)']:.4f}")
        print(f"PYSPARK_X2_TIME={avg_times['PySpark(x2)']:.4f}")

    print("=" * 60)


if __name__ == "__main__":
    run_benchmark()