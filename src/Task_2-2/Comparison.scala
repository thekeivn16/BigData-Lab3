import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object Comparison {

  def compute(
      approxResult: DataFrame,
      exactResult: DataFrame,
      percentile: String
  ): DataFrame = {

    require(
      percentile == "p80" || percentile == "p90",
      "percentile must be either p80 or p90."
    )

    approxResult
      .alias("approx")
      .join(
        exactResult.alias("exact"),
        Seq("SKU", "year_month"),
        "inner"
      )
      .select(
        col("SKU"),
        col("year_month"),
        lit(percentile.toUpperCase).as("percentile"),
        col("approx.order_count").as("order_count"),

        col(s"approx.$percentile").as("approx_threshold"),
        col(s"exact.$percentile").as("exact_threshold"),

        col(s"approx.${percentile}_order_count")
          .as("approx_selected"),

        col(s"exact.${percentile}_order_count")
          .as("exact_selected"),

        col(s"approx.${percentile}_stddev")
          .as("approx_stddev"),

        col(s"exact.${percentile}_stddev")
          .as("exact_stddev")
      )
      .withColumn(
        "threshold_diff",
        abs(col("approx_threshold") - col("exact_threshold"))
      )
      .withColumn(
        // Both methods filter the same rows using promotion_count >= threshold.
        // Their selected sets are nested, so this counts changed memberships.
        "changed_order_count",
        abs(col("approx_selected") - col("exact_selected"))
      )
      .withColumn(
        "stddev_diff",
        abs(col("approx_stddev") - col("exact_stddev"))
      )
  }

  def summarize(comparison: DataFrame): DataFrame = {
    // Ignore negligible floating-point differences in numeric comparisons.
    val tolerance = 1e-9

    comparison
      .groupBy("percentile")
      .agg(
        count(lit(1)).as("total_groups"),

        count(
          when(col("threshold_diff") > tolerance, lit(1))
        ).as("threshold_changed_groups"),

        count(
          when(col("changed_order_count") > 0, lit(1))
        ).as("set_changed_groups"),

        count(
          when(col("stddev_diff") > tolerance, lit(1))
        ).as("stddev_changed_groups"),

        avg(col("threshold_diff")).as("mean_threshold_diff")
      )
      .orderBy("percentile")
  }
}