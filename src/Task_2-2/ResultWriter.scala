import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

object ResultWriter {

  def write(
      approxResult: DataFrame,
      exactResult: DataFrame,
      outputDirectory: String
  ): Unit = {

    // Identify the method used to produce each result row.
    val approx = approxResult.withColumn("method", lit("approx"))
    val exact = exactResult.withColumn("method", lit("exact"))

    // Preserve both methods in the same output dataset.
    val combined = approx.unionByName(exact)

    // Write one data file inside the output directory.
    combined
      .coalesce(1)
      .write
      .mode("overwrite")
      .option("maxRecordsPerFile", "0")
      .parquet(outputDirectory)
  }
}