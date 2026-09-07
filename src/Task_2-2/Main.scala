import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main {
  def main(args: Array[String]): Unit = {
    require(
      args.length == 2,
      "Usage: Main <input-csv> <output-directory>"
    )

    val spark = SparkSession.builder()
      .appName("Task-2-2")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      val raw = spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("mode", "FAILFAST")
        .csv(args(0))

      val prepared = DataPreparation.prepare(raw)

      println(s"Application ID: ${spark.sparkContext.applicationId}")
      println(s"Execution master: ${spark.sparkContext.master}")

      println("SKU-month group statistics:")

      prepared
        .groupBy("SKU", "year_month")
        .count()
        .agg(
          count(lit(1)).as("total_groups"),
          max(col("count")).as("largest_group"),
          count(
            when(col("count") > 1000, lit(1))
          ).as("groups_over_1000")
        )
        .show(false)

      // Compute approximate thresholds and amount statistics.
      val approxThresholds = ApproxPercentile.compute(prepared)

      val approxResult = AmountStatistics.compute(
        prepared,
        approxThresholds
      )

      // Compute exact thresholds and reuse the same statistics implementation.
      val exactThresholds = ExactPercentile.compute(prepared)

      val exactResult = AmountStatistics.compute(
        prepared,
        exactThresholds
      )

      // Compare both percentile levels using the same implementation.
      val p80Comparison = Comparison.compute(
        approxResult,
        exactResult,
        "p80"
      )

      val p90Comparison = Comparison.compute(
        approxResult,
        exactResult,
        "p90"
      )

      val comparison = p80Comparison.unionByName(p90Comparison)

      // Show one summary row for each percentile level.
      println("Comparison summary:")
      Comparison.summarize(comparison).show(false)

      // Show groups with the largest differences in selected orders.
      println("Groups with different selected orders:")
      comparison
      .filter(col("changed_order_count") > 0)
      .orderBy(
        desc("changed_order_count"),
        col("SKU"),
        col("year_month"),
        col("percentile")
      )
      .show(10, false)

      println(s"Benchmark master: ${spark.sparkContext.master}")
      println(s"Spark version: ${spark.version}")
      println(
        s"Shuffle partitions: ${spark.conf.get("spark.sql.shuffle.partitions")}"
      )
      println(
        s"AQE enabled: ${spark.conf.get("spark.sql.adaptive.enabled")}"
      )

      val measurements = Benchmark.run(prepared, runs = 5)

      println("Individual benchmark measurements:")
      measurements
        .orderBy("run", "execution_order")
        .show(10, false)

      println("Benchmark summary:")
      Benchmark.summarize(measurements).show(false)
      // Export results outside the benchmark's measured intervals.
      ResultWriter.write(
        approxResult,
        exactResult,
        args(1)
      )

      println(s"Results written to: ${args(1)}")
    } finally {
      spark.stop()
    }
  }
}