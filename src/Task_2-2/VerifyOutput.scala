import java.nio.file.{Files, Paths}
import org.apache.spark.sql.SparkSession

object VerifyOutput {

  def main(args: Array[String]): Unit = {
    require(args.length == 1, "Usage: VerifyOutput <parquet-file>")

    val path = Paths.get(args(0)).toAbsolutePath.normalize()

    require(
      Files.isRegularFile(path),
      "The output must be a regular file, not a directory."
    )

    val spark = SparkSession.builder()
      .appName("Task-2-2-Output-Verification")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try {
      // Read the exact local file intended for submission.
      val result = spark.read.parquet(path.toUri.toString)

      result.printSchema()
      result.groupBy("method").count().show(false)

      val rowCount = result.count()
      val uniqueKeyCount = result
        .select("SKU", "year_month", "method")
        .distinct()
        .count()

      // These expectations apply to the supplied dataset and chosen schema.
      require(rowCount == 32972L, "Unexpected output row count.")
      require(
        uniqueKeyCount == rowCount,
        "Duplicate SKU-month-method keys found."
      )

      println(s"Verified rows: $rowCount")
      println("PARQUET_READBACK_OK")

    } finally {
      spark.stop()
    }
  }
}