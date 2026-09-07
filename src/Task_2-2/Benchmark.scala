import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Benchmark {

  def run(
      prepared: DataFrame,
      runs: Int = 5
  ): DataFrame = {

    require(runs >= 5, "At least 5 measured runs are required.")

    val spark = prepared.sparkSession
    import spark.implicits._

    // Cache only the shared input columns, not method-specific results.
    val input = prepared
      .select("SKU", "year_month", "promotion_count", "amount_value")
      .cache()

    // Alternate execution order between rounds.
    def methodOrder(round: Int): Seq[String] = {
      if (round % 2 == 1) Seq("approx", "exact")
      else Seq("exact", "approx")
    }

    def execute(method: String): Unit = {
      // Build a fresh query for each execution.
      val thresholds = method match {
        case "approx" => ApproxPercentile.compute(input)
        case "exact"  => ExactPercentile.compute(input)
        case other =>
          throw new IllegalArgumentException(s"Unknown method: $other")
      }

      // Materialize every output row and column.
      // Only the aggregated result is collected, not the source dataset.
      AmountStatistics.compute(input, thresholds).collect()
      ()
    }

    try {
      // Load the shared input before starting any measurements.
      input.count()

      println("Running warm-up rounds...")

      for {
        round <- 1 to 2
        method <- methodOrder(round)
      } {
        execute(method)
      }

      val measurements = (1 to runs).flatMap { round =>
        methodOrder(round).zipWithIndex.map {
          case (method, position) =>
            val start = System.nanoTime()

            execute(method)

            val seconds = (System.nanoTime() - start) / 1e9

            // Logging is outside the measured interval.
            println(s"$method run $round: $seconds seconds")

            (method, round, position + 1, seconds)
        }
      }

      measurements.toDF(
        "method",
        "run",
        "execution_order",
        "seconds"
      )

    } finally {
      input.unpersist(blocking = true)
    }
  }

  def summarize(measurements: DataFrame): DataFrame = {
    measurements
      .groupBy("method")
      .agg(
        count(lit(1)).as("measured_runs"),
        avg(col("seconds")).as("mean_seconds"),
        stddev_samp(col("seconds")).as("stddev_seconds")
      )
      .orderBy("method")
  }
}