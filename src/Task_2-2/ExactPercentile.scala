import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._

object ExactPercentile {

  def compute(prepared: DataFrame): DataFrame = {
    val grouped = prepared
      .groupBy("SKU", "year_month")
      .agg(
        count(lit(1)).as("order_count"),

        // Keep duplicate counts and sort them in ascending order.
        sort_array(
          collect_list(col("promotion_count"))
        ).as("sorted_counts")
      )

    grouped.select(
      col("SKU"),
      col("year_month"),
      col("order_count"),
      interpolate(0.8).as("p80"),
      interpolate(0.9).as("p90")
    )
  }

  private def interpolate(probability: Double): Column = {
    // Compute the zero-based percentile position.
    val position =
      (col("order_count").cast("double") - lit(1.0)) *
        lit(probability)

    val lowerIndex = floor(position).cast("int")
    val upperIndex = ceil(position).cast("int")

    // element_at uses one-based array indexing.
    val lowerValue = element_at(
      col("sorted_counts"),
      lowerIndex + lit(1)
    ).cast("double")

    val upperValue = element_at(
      col("sorted_counts"),
      upperIndex + lit(1)
    ).cast("double")

    // Interpolate between the two neighboring values.
    val weight = position - lowerIndex.cast("double")

    lowerValue + weight * (upperValue - lowerValue)
  }
}