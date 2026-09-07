import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object DataPreparation {

  def prepare(raw: DataFrame): DataFrame = {
    // Thay promotion-ids null bằng chuỗi rỗng rồi tách các mã.
    val promotionIds =
      split(coalesce(col("promotion-ids"), lit("")), ",")

    // Bỏ phần tử rỗng hoặc chỉ chứa khoảng trắng.
    val nonEmptyPromotionIds =
      filter(promotionIds, id => length(trim(id)) > 0)

    raw
      .withColumn(
        "order_date",
        to_date(col("Date"), "MM-dd-yy")
      )
      .withColumn(
        "year_month",
        date_format(col("order_date"), "yyyy-MM")
      )
      .withColumn(
        "amount_value",
        col("Amount").cast("double")
      )
      .withColumn(
        "promotion_count",
        size(nonEmptyPromotionIds)
      )
  }
}