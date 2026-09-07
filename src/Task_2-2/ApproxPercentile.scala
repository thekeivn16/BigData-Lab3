import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ApproxPercentile {

  def compute(
      prepared: DataFrame,
      accuracy: Int = 10000
  ): DataFrame = {

    require(accuracy > 0, "accuracy must be greater than 0.")

    prepared
      .groupBy("SKU", "year_month")
      .agg(
        // Count all rows in the group, including rows with missing Amount.
        count(lit(1)).as("order_count"),

        // Compute P80 and P90 of promotion counts in a single aggregation.
        percentile_approx(
          col("promotion_count"),
          array(lit(0.8), lit(0.9)),
          lit(accuracy)
        ).as("percentiles")
      )
      .select(
        col("SKU"),
        col("year_month"),
        col("order_count"),

        // Extract thresholds in the same order as the input: [P80, P90].
        col("percentiles").getItem(0).cast("double").as("p80"),
        col("percentiles").getItem(1).cast("double").as("p90")
      )
  }
}