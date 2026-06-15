import time
import pandas as pd

# 先手动下载数据到本地
DATA_PATH = "./douban_movies.csv"
RUNS = 3


def load_data_pandas():
    df = pd.read_csv(DATA_PATH, sep=",", quotechar='"', header=0)
    df = df[df["movie_id"].notna()]
    df["rating_score"] = pd.to_numeric(df["rating_score"], errors="coerce")
    df["genres"] = df["genres"].fillna("Unknown")
    df["genres"] = df["genres"].replace("[]", "Unknown")
    return df


def query1_pandas(df):
    df_exploded = df.copy()
    df_exploded = df_exploded[df_exploded["genres"] != "Unknown"]
    rows = []
    for _, row in df_exploded.iterrows():
        for genre in str(row["genres"]).split("/"):
            genre = genre.strip()
            if genre:
                rows.append({"genre": genre, "rating_score": row["rating_score"]})
    df_genre = pd.DataFrame(rows)
    df_genre = df_genre[
        (df_genre["rating_score"].notna()) &
        (df_genre["genre"].notna()) &
        (df_genre["genre"] != "")
        ]
    result = df_genre.groupby("genre").agg(
        count=("genre", "count"),
        avg_rating=("rating_score", "mean")
    ).reset_index()
    result["avg_rating"] = result["avg_rating"].round(2)
    result = result.sort_values("avg_rating", ascending=False).head(10)
    return result


def run_benchmark():
    print("=" * 60)
    print("Query 1 Performance Benchmark: Pandas Only")
    print("=" * 60)

    print("\n[1/2] Loading data...")
    df_pd = load_data_pandas()
    print(f"    Loaded {len(df_pd)} rows")

    times = {"Pandas": []}

    print("\n[2/2] Running Pandas benchmark...")
    for i in range(RUNS):
        start = time.time()
        result_pd = query1_pandas(df_pd)
        times["Pandas"].append(time.time() - start)
        print(f"    Run {i + 1}/{RUNS}: {times['Pandas'][-1]:.3f}s")

    avg_time = sum(times["Pandas"]) / len(times["Pandas"])

    print("\n" + "=" * 60)
    print("FINAL_RESULTS:")
    print("=" * 60)
    print(f"PANDAS_TIME={avg_time:.4f}")
    print("=" * 60)


if __name__ == "__main__":
    run_benchmark()