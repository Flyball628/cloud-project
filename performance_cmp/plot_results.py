import matplotlib.pyplot as plt

# 手动填入从两个脚本获得的结果
PANDAS_TIME = 1.2709  # 填入实际值
PYSPARK_X1_TIME = 0.2297  # 填入实际值
PYSPARK_X2_TIME = 0.2081  # 填入实际值


def plot_results():
    avg_times = {
        "Pandas": PANDAS_TIME,
        "PySpark(x1)": PYSPARK_X1_TIME,
        "PySpark(x2)": PYSPARK_X2_TIME
    }

    labels = list(avg_times.keys())
    values = list(avg_times.values())
    colors = ["#3498db", "#2ecc71", "#e74c3c"]

    fig, ax = plt.subplots(figsize=(10, 6))
    bars = ax.bar(labels, values, color=colors, edgecolor="black", linewidth=1.2)

    for bar, val in zip(bars, values):
        ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05,
                f"{val:.3f}s", ha="center", va="bottom", fontsize=12, fontweight="bold")

    ax.set_ylabel("Execution Time (seconds)", fontsize=12)
    ax.set_title("Query 1 Performance: Pandas vs PySpark\n( GROUP BY Genre Average Rating )", fontsize=14)
    ax.set_ylim(0, max(values) * 1.2)
    ax.grid(axis="y", alpha=0.3)
    plt.tight_layout()
    plt.savefig("query1_performance_comparison.png", dpi=150)
    print("Chart saved: query1_performance_comparison.png")

    print("\nAmdahl's Law Analysis:")
    print("=" * 60)
    t1 = avg_times["PySpark(x1)"]
    t2 = avg_times["PySpark(x2)"]
    if t2 > 0 and t1 > 0:
        speedup = t1 / t2
        ideal_speedup = 2.0
        efficiency = (speedup / ideal_speedup) * 100
        print(f"  Actual Speedup (x1 -> x2): {speedup:.2f}x")
        print(f"  Ideal Speedup: {ideal_speedup:.2f}x")
        print(f"  Parallel Efficiency: {efficiency:.1f}%")


if __name__ == "__main__":
    plot_results()