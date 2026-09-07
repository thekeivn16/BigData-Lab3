import java.lang.Iterable
import java.net.URI
import java.time.LocalDate
import java.time.format.DateTimeFormatter

import scala.collection.mutable
import scala.io.Source

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


class BucketMapper
  extends Mapper[LongWritable, Text, Text, Text] {

  private val DATE_FMT =
    DateTimeFormatter.ofPattern("MM-dd-yy")

  // Column indexes in Amazon Sale Report
  private val COL_DATE   = 2
  private val COL_STATUS = 3
  private val COL_SIZE   = 10
  private val COL_QTY    = 13
  private val COL_AMOUNT = 15
  private val COL_STATE  = 17

  // State -> total bought-order count from Job 0
  private val stateCounts =
    mutable.Map[String, Long]()

  /*
   * Load the small state-count table distributed
   * to each mapper through Hadoop Distributed Cache.
   */
  override def setup(
      context: Mapper[LongWritable, Text, Text, Text]#Context
  ): Unit = {

    val source = Source.fromFile("state_counts")

    try {
      source.getLines().foreach { line =>
        val parts = line.split("\t", -1)

        if (parts.length == 2) {
          val state = parts(0).trim.toUpperCase

          try {
            val count = parts(1).trim.toLong
            stateCounts(state) = count
          } catch {
            case _: Exception =>
          }
        }
      }
    } finally {
      source.close()
    }
  }

  // Parse one CSV line while respecting quoted commas
  private def parseCSV(line: String): Array[String] = {
    line
      .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      .map(_.trim.replaceAll("^\"|\"$", ""))
  }

  private def parseDate(s: String): Option[LocalDate] = {
    try {
      Some(LocalDate.parse(s.trim, DATE_FMT))
    } catch {
      case _: Exception => None
    }
  }

  /*
   * An order is bought when:
   * - Status contains "shipped"
   * - Qty != 0
   */
  private def isBought(
      status: String,
      qtyString: String
  ): Boolean = {

    try {
      val qty = qtyString.trim.toDouble

      status.trim.toLowerCase.contains("shipped") &&
      qty != 0.0
    } catch {
      case _: Exception => false
    }
  }

  /*
   * Explicit null policy for purchased Amount:
   * missing/invalid Amount is treated as 0.0.
   */
  private def parseAmount(
      amountString: String
  ): (Double, Boolean) = {

    try {
      if (amountString.trim.isEmpty) {
        (0.0, false)
      } else {
        (amountString.trim.toDouble, true)
      }
    } catch {
      case _: Exception => (0.0, false)
    }
  }

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, Text]#Context
  ): Unit = {

    val line = value.toString

    // Skip header
    if (line.toLowerCase.startsWith("index,")) {
      return
    }

    val cols = parseCSV(line)

    // Validate the expected 24-column schema
    if (cols.length == 24) {

      val dateString   = cols(COL_DATE)
      val status       = cols(COL_STATUS)
      val size         = cols(COL_SIZE).trim
      val qtyString    = cols(COL_QTY)
      val amountString = cols(COL_AMOUNT)
      val state        = cols(COL_STATE).trim.toUpperCase

      if (
        state.nonEmpty &&
        size.nonEmpty &&
        isBought(status, qtyString)
      ) {

        parseDate(dateString) match {

          case Some(orderDate) =>

            stateCounts.get(state) match {

              case Some(totalBoughtOrders) =>

                val windowLength =
                  if (totalBoughtOrders > 10000L) 5
                  else 10

                val (amount, hasAmount) =
                  parseAmount(amountString)

                if (!hasAmount) {
                  context
                    .getCounter(
                      "Task1_1",
                      "MISSING_AMOUNT"
                    )
                    .increment(1L)
                }

                context
                  .getCounter(
                    "Task1_1",
                    "BOUGHT_RECORDS"
                  )
                  .increment(1L)

                /*
                 * Map this order to every future
                 * window whose history contains it.
                 *
                 * Order at t contributes to:
                 * t+1, ..., t+windowLength
                 */
                var offset = 1

                while (offset <= windowLength) {

                  val windowDate =
                    orderDate.plusDays(offset)

                  val bucketKey =
                    s"$state|${windowDate.toString}|$size"

                  val bucketValue =
                    s"1|$amount|${amount * amount}"

                  context.write(
                    new Text(bucketKey),
                    new Text(bucketValue)
                  )

                  context
                    .getCounter(
                      "Task1_1",
                      "BUCKET_EMISSIONS"
                    )
                    .increment(1L)

                  offset += 1
                }

              case None =>

                context
                  .getCounter(
                    "Task1_1",
                    "STATE_COUNT_MISSING"
                  )
                  .increment(1L)
            }

          case None =>

            context
              .getCounter(
                "Task1_1",
                "INVALID_DATE"
              )
              .increment(1L)
        }
      }
    }
  }
}


/*
 * Both the Combiner and Reducer perform exactly
 * the same associative aggregation:
 *
 * (count, sumAmount, sumAmountSquared)
 */
class BucketStatsReducer
  extends Reducer[Text, Text, Text, Text] {

  override def reduce(
      key: Text,
      values: Iterable[Text],
      context: Reducer[Text, Text, Text, Text]#Context
  ): Unit = {

    var totalCount = 0L
    var sumAmount = 0.0
    var sumAmountSquared = 0.0

    val iterator = values.iterator()

    while (iterator.hasNext) {

      val parts =
        iterator.next().toString.split("\\|", -1)

      if (parts.length == 3) {
        try {
          totalCount += parts(0).toLong
          sumAmount += parts(1).toDouble
          sumAmountSquared += parts(2).toDouble
        } catch {
          case _: Exception =>
        }
      }
    }

    val aggregatedValue =
      s"$totalCount|$sumAmount|$sumAmountSquared"

    context.write(
      key,
      new Text(aggregatedValue)
    )
  }
}


object BucketAggregationJob {

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println(
        "Usage: BucketAggregationJob " +
        "<input_path> <state_counts_file> <output_path>"
      )
      System.exit(1)
    }

    val conf = new Configuration()

    val job = Job.getInstance(
      conf,
      "Task 1-1 Job 1 - Map To Window Buckets"
    )

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[BucketMapper])

    /*
     * Summation is associative and commutative,
     * therefore the same aggregation can safely
     * be used as Combiner and Reducer.
     */
    job.setCombinerClass(
      classOf[BucketStatsReducer]
    )

    job.setReducerClass(
      classOf[BucketStatsReducer]
    )

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[Text])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[Text])

    /*
     * Add Job 0's small output file to
     * Hadoop Distributed Cache.
     */
    val stateCountsPath =
      new Path(args(1))

    val fs =
      stateCountsPath.getFileSystem(conf)

    val qualifiedStateCountsPath =
      fs.makeQualified(stateCountsPath)

    job.addCacheFile(
      new URI(
        qualifiedStateCountsPath.toString +
        "#state_counts"
      )
    )

    FileInputFormat.addInputPath(
      job,
      new Path(args(0))
    )

    FileOutputFormat.setOutputPath(
      job,
      new Path(args(2))
    )

    val success =
      job.waitForCompletion(true)

    System.exit(
      if (success) 0 else 1
    )
  }
}
