import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object Task21 {
  private val inputSchema = StructType(
    Seq(
      "index",
      "Order ID",
      "Date",
      "Status",
      "Fulfilment",
      "Sales Channel ",
      "ship-service-level",
      "Style",
      "SKU",
      "Category",
      "Size",
      "ASIN",
      "Courier Status",
      "Qty",
      "currency",
      "Amount",
      "ship-city",
      "ship-state",
      "ship-postal-code",
      "ship-country",
      "promotion-ids",
      "B2B",
      "fulfilled-by",
      "Unnamed: 22"
    ).map(name => StructField(name, StringType, nullable = true))
  )

  def main(args: Array[String]): Unit = {
    require(
      args.length == 2 || args.length == 3,
      "Usage: Task21 <input-csv-path> <temporary-output-directory> " +
        "[benchmark-run-number]"
    )

    val inputPath = args(0)
    val outputPath = args(1)
    val benchmarkRunNumber = args.lift(2).map(_.toInt).getOrElse(1)
    require(benchmarkRunNumber >= 1, "Benchmark run number must be positive")

    val spark = SparkSession.builder()
      .appName("Task-2-1")
      .getOrCreate()

    val sc = spark.sparkContext
    sc.setLogLevel("WARN")

    // Read as strings, then cast only the fields required by the query.
    val raw = spark.read
      .option("header", "true")
      .option("mode", "PERMISSIVE")
      .schema(inputSchema)
      .csv(inputPath)

    // Promotion occurrences are associated with their Order ID.
    val base = raw.select(
      trim(col("Order ID")).as("order_id"),
      to_date(trim(col("Date")), "MM-dd-yy").as("order_date"),
      lower(trim(col("Status"))).as("status"),
      lower(trim(col("Fulfilment"))).as("fulfilment"),
      lower(trim(col("ship-service-level"))).as("service_level"),
      lower(trim(col("Courier Status"))).as("courier_status"),
      upper(trim(col("ship-city"))).as("city"),
      upper(trim(col("ship-state"))).as("state"),
      col("Amount").cast(DoubleType).as("amount"),
      col("promotion-ids").as("promotion_ids_raw")
    ).filter(col("order_id").isNotNull)

    // Retain all promotion identifiers, including Amazon-issued promotions.
    // Remove blank tokens and duplicate identifiers within the same record.
    val orders = base.withColumn(
      "promotion_ids",
      array_distinct(
        filter(
          transform(
            split(coalesce(col("promotion_ids_raw"), lit("")), ","),
            promotionId => trim(promotionId)
          ),
          promotionId => length(promotionId) > 0
        )
      )
    )

    // One record-promotion association per row.
    val promotionAppearances = orders
      .select(
        col("order_id"),
        col("order_date"),
        explode(col("promotion_ids")).as("promotion_id")
      )
      .filter(
        col("order_date").isNotNull &&
          length(col("promotion_id")) > 0
      )

    // A promotion is valid when the number of days between its global first and last appearances is at least two days.
    val validPromotions = promotionAppearances
      .groupBy("promotion_id")
      .agg(
        min("order_date").as("first_appearance"),
        max("order_date").as("last_appearance")
      )
      .withColumn(
        "active_days",
        datediff(col("last_appearance"), col("first_appearance"))
      )
      .filter(col("active_days") >= 2)
      .select("promotion_id")

    // Count valid promotion occurrences per Order ID.
    val validPromotionCounts = promotionAppearances
      .join(validPromotions, Seq("promotion_id"), "inner")
      .groupBy("order_id")
      .agg(count("promotion_id").as("valid_promotion_count"))
      .filter(col("valid_promotion_count") >= 3)

    // Compute the state benchmark from Merchant-fulfilment records whose Courier Status is Shipped. AVG ignores null amounts.
    val stateMerchantShippedAverages = orders
      .filter(
        col("fulfilment") === "merchant" &&
          col("courier_status") === "shipped" &&
          col("state").isNotNull &&
          col("amount").isNotNull
      )
      .groupBy("state")
      .agg(avg("amount").as("state_merchant_shipped_average"))

    // The denominator is all Cancelled + Standard records in each city.
    // LEFT joins retain orders that have no promotions or no state threshold.
    val evaluatedStandardOrders = orders
      .filter(
        col("service_level") === "standard" &&
          col("status").contains("cancelled") &&
          col("city").isNotNull
      )
      .join(validPromotionCounts, Seq("order_id"), "left")
      .withColumn(
        "valid_promotion_count",
        coalesce(col("valid_promotion_count"), lit(0L))
      )
      .join(stateMerchantShippedAverages, Seq("state"), "left")
      .withColumn(
        "is_qualifying",
        col("valid_promotion_count") >= 3 &&
          col("state_merchant_shipped_average").isNotNull &&
          coalesce(col("amount"), lit(0.0)) <
            col("state_merchant_shipped_average")
      )

    val result = evaluatedStandardOrders
      .groupBy("city")
      .agg(
        count(lit(1)).as("cancelled_standard_orders"),
        sum(when(col("is_qualifying"), lit(1L)).otherwise(lit(0L)))
          .as("qualifying_orders")
      )
      .withColumn(
        "percentage",
        col("qualifying_orders").cast(DoubleType) /
          col("cancelled_standard_orders").cast(DoubleType) * 100.0
      )
      .select("city", "percentage")

    // Required extended plan for the written report.
    println("========== EXTENDED EXECUTION PLAN ==========")
    result.explain(true)

    // One timed final-write action per YARN application. The README submits five separate applications so ResourceManager metrics are available for every benchmark run.
    val jobGroup = s"task21-yarn-benchmark-$benchmarkRunNumber"
    sc.setJobGroup(jobGroup, s"Task 2-1 YARN run $benchmarkRunNumber")

    val startedAt = System.nanoTime()

    result
      .coalesce(1)
      .write
      .mode("overwrite")
      .parquet(outputPath)

    val elapsedSeconds =
      (System.nanoTime() - startedAt).toDouble / 1000000000.0

    println("========== YARN BENCHMARK RUN ==========")
    println(s"Benchmark run number: $benchmarkRunNumber")
    println(f"Final-write action time: $elapsedSeconds%.3f seconds")

    val tracker = sc.statusTracker
    val jobIds = tracker.getJobIdsForGroup(jobGroup).distinct.sorted
    val stageIds = jobIds
      .flatMap(jobId => tracker.getJobInfo(jobId).toSeq)
      .flatMap(jobInfo => jobInfo.stageIds)
      .distinct
      .sorted

    println(s"YARN run job IDs: ${jobIds.mkString(", ")}")
    println(s"YARN run stage IDs: ${stageIds.mkString(", ")}")
    println(s"YARN run distinct stage count: ${stageIds.length}")
    println(s"Parquet output directory: $outputPath")

    // Display the final materialized output without executing the full source
    // pipeline for a sixth time.
    sc.clearJobGroup()
    println("========== SAMPLE RESULT ==========")
    spark.read.parquet(outputPath).show(50, truncate = false)

    spark.stop()
  }
}
