import math
import multiprocessing
import os
import time

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StructField, StructType, StringType

METRICS = ["size", "atime", "ctime", "mtime"]


# ---------- Spark helpers ----------


def get_system_memory_gb() -> int:
    """Return total system memory in GB (rounded down)."""
    try:
        import psutil

        total_bytes = psutil.virtual_memory().total
    except ImportError:
        meminfo = {}
        with open("/proc/meminfo") as f:
            for line in f:
                key, value = line.split(":", 1)
                meminfo[key.strip()] = value.strip()
        total_kb = int(meminfo["MemTotal"].split()[0])
        total_bytes = total_kb * 1024

    return max(1, math.floor(total_bytes / (1024**3)))


def create_spark(
    app_name: str = "FsCsvProcess", mem_fraction: float = 1
) -> SparkSession:
    """Create a local SparkSession using all cores and ~mem_fraction of RAM."""
    cores = multiprocessing.cpu_count()
    total_gb = get_system_memory_gb()
    use_gb = max(1, int(total_gb * mem_fraction))
    mem_str = f"{use_gb}g"

    print(f"[Spark] {cores} cores, {total_gb} GB RAM, using {mem_str}")

    return (
        SparkSession.builder.appName(app_name)
        .master(f"local[{cores}]")
        .config("spark.driver.memory", mem_str)
        .config("spark.executor.memory", mem_str)
        .config("spark.executor.cores", str(cores))
        # tuned parallelism for local mode
        .config("spark.default.parallelism", str(cores * 2))
        .config("spark.sql.shuffle.partitions", str(cores * 2))
        .getOrCreate()
    )


# ---------- Stats helpers ----------


def write_sorted_values(
    df,
    dataset_name: str,
    base_dir: str,
    group_col: str,
):
    """
    For each group_col and each metric (size, atime, ctime, mtime),
    write out all values sorted within the group, with their rank.

    Output Parquet schema:
        group_col
        metric
        value
        rank_1based
        group_size
    """
    # Build array of structs:
    metric_array = F.array(
        *[F.struct(F.lit(m).alias("metric"), F.col(m).alias("value")) for m in METRICS]
    )

    long_df = df.select(F.col(group_col), F.explode(metric_array).alias("m")).select(
        F.col(group_col),
        F.col("m.metric").alias("metric"),
        F.col("m.value").alias("value"),
    )

    # Precompute group sizes ONCE from original df
    group_sizes = df.groupBy(group_col).agg(F.count("*").alias("group_size"))

    w_group_metric = Window.partitionBy(group_col, "metric").orderBy("value")

    ranked = long_df.withColumn("rank_1based", F.row_number().over(w_group_metric))

    out = ranked.join(group_sizes, on=group_col, how="left").select(
        group_col, "metric", "value", "rank_1based", "group_size"
    )

    out_dir = os.path.join(base_dir, f"{dataset_name}-{group_col}-sorted-parquet")
    (
        out.write.mode("overwrite")
        .option("compression", "snappy")  # good balance of speed & size
        .parquet(out_dir)
    )
    print(f"[{dataset_name}] Sorted exact values written to Parquet dir: {out_dir}")


# ---------- Core dataset processor ----------


def process_dataset(
    spark: SparkSession,
    dataset_name: str,
    input_folder: str,
    base_dir: str,
):
    """
    Split into at most 9 parts:
        parts[0..7] = type, mode, uid, gid, size, atime, ctime, mtime
        parts[8]    = everything else → ignored

    Works uniformly for itap, hpss, nersc.
    Writes *parallel* Parquet outputs (multiple part-* files).
    """
    start_time = time.perf_counter()
    print(f"\n=== Processing dataset: {dataset_name} ===")
    print(f"Input folder: {input_folder}")

    # Choose schema: itap/hpss → 9 cols, nersc → 11 cols. Extra columns are ignored.
    base_cols = ["type", "mode", "uid", "gid", "size", "atime", "ctime", "mtime"]
    if dataset_name.lower() in ("itap", "hpss"):
        schema = StructType(
            [StructField(c, StringType(), True) for c in base_cols + ["extra1"]]
        )
    elif dataset_name.lower() == "nersc":
        schema = StructType(
            [
                StructField(c, StringType(), True)
                for c in base_cols + ["extra1", "extra2", "extra3"]
            ]
        )
    else:
        raise ValueError(f"Unknown dataset_name for schema selection: {dataset_name}")

    raw_df = spark.read.csv(
        input_folder,
        schema=schema,
        sep=",",
        quote='"',
        escape='"',
        multiLine=True,
        header=False,
        ignoreLeadingWhiteSpace=True,
        ignoreTrailingWhiteSpace=True,
    )

    # Track basic counts; any row missing required cols is considered invalid.
    counts = raw_df.selectExpr("count(*) as total_rows").collect()[0]
    valid_df = raw_df
    for col in base_cols:
        valid_df = valid_df.where(F.col(col).isNotNull())
    valid_rows = valid_df.count()
    invalid_rows = counts.total_rows - valid_rows

    print(f"[{dataset_name}] Total rows: {counts.total_rows}")
    print(f"[{dataset_name}] Valid rows (>=8 cols): {valid_rows}")
    print(f"[{dataset_name}] Invalid rows: {invalid_rows}")

    df = raw_df.select(*[F.col(c).alias(c) for c in base_cols])

    # Cast numeric columns to integer types (file sizes and unix timestamps)
    for c in ("size", "atime", "ctime", "mtime"):
        df = df.withColumn(c, F.col(c).cast("long"))

    # Optional: repartition by uid to help later groupBy/window ops
    # num_partitions = spark.sparkContext.defaultParallelism
    # df = df.repartition(num_partitions, "uid")

    # Sorted exact values for rank-error computation (Parquet)
    write_sorted_values(df, dataset_name, base_dir, "gid")
    write_sorted_values(df, dataset_name, base_dir, "uid")

    elapsed = time.perf_counter() - start_time
    print(f"[{dataset_name}] Elapsed time: {elapsed:.2f} seconds")
    print(f"=== Done: {dataset_name} ===\n")


# ---------- Main ----------

if __name__ == "__main__":
    # for i in range[1, 2, 3]:
    #     spark = create_spark()
    #     process_dataset(
    #         spark,
    #         "itap",
    #         "/home/ubuntu/purdue-index-analysis/new_processing/csv/itap/1m",
    #         f"pyspark128-out{i}/itap",
    #     )
    #     spark.stop()

    # for i in [1, 2, 3]:
    #     spark = create_spark()
    #     process_dataset(
    #         spark,
    #         "nersc",
    #         "/home/ubuntu/purdue-index-analysis/new_processing/csv/nersc",
    #         f"pyspark128-out{i}/nersc",
    #     )
    #     spark.stop()

    i = 3
    spark = create_spark()
    process_dataset(
        spark,
        "hpss",
        "/home/ubuntu/purdue-index-analysis/new_processing/csv/hpss",
        f"pyspark128-out{i}/hpss",
    )
    spark.stop()
