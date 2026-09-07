import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object AmountStatistics {

  def compute(
      prepared: DataFrame,
      thresholds: DataFrame
  ): DataFrame = {

    // Attach each group's thresholds to its order rows.
    val joined = prepared.join(
      thresholds,
      Seq("SKU", "year_month"),
      "inner"
    )

    val qualifiesP80 = col("promotion_count") >= col("p80")
    val qualifiesP90 = col("promotion_count") >= col("p90")

    joined
      .groupBy("SKU", "year_month", "order_count", "p80", "p90")
      .agg(
        // Count qualifying rows, including rows with missing amounts.
        count(when(qualifiesP80, lit(1))).as("p80_order_count"),

        // Count only qualifying rows with non-null amounts.
        count(
          when(qualifiesP80, col("amount_value"))
        ).as("p80_amount_count"),

        // Compute population standard deviation; replace a null result with zero.
        coalesce(
          stddev_pop(when(qualifiesP80, col("amount_value"))),
          lit(0.0)
        ).as("p80_stddev"),

        count(when(qualifiesP90, lit(1))).as("p90_order_count"),

        count(
          when(qualifiesP90, col("amount_value"))
        ).as("p90_amount_count"),

        coalesce(
          stddev_pop(when(qualifiesP90, col("amount_value"))),
          lit(0.0)
        ).as("p90_stddev")
      )
  }
}