import java.lang.Iterable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, NullWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


/*
 * Input from Job 1:
 *
 * state|window_date|size    count|sumAmount|sumAmountSquared
 *
 * Mapper changes the grouping key to:
 *
 * state|window_date
 *
 * so that all candidate sizes of the same state/date
 * arrive at the same reducer call.
 */
class WinnerMapper
  extends Mapper[LongWritable, Text, Text, Text] {

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, Text]#Context
  ): Unit = {

    val line = value.toString

    // Separate Job 1 key and value by the Hadoop tab delimiter
    val parts = line.split("\t", 2)

    if (parts.length == 2) {

      val bucketKey =
        parts(0).split("\\|", -1)

      val stats =
        parts(1).split("\\|", -1)

      if (
        bucketKey.length == 3 &&
        stats.length == 3
      ) {

        val state =
          bucketKey(0)

        val windowDate =
          bucketKey(1)

        val size =
          bucketKey(2)

        try {

          val count =
            stats(0).toLong

          val sumAmount =
            stats(1).toDouble

          val sumAmountSquared =
            stats(2).toDouble

          val outputKey =
            s"$state|$windowDate"

          val outputValue =
            s"$size|$count|$sumAmount|$sumAmountSquared"

          context.write(
            new Text(outputKey),
            new Text(outputValue)
          )

        } catch {
          case _: Exception =>
        }
      }
    }
  }
}


class WinnerReducer
  extends Reducer[Text, Text, NullWritable, Text] {

  /*
   * Because this job uses exactly one reducer,
   * writing the header in setup produces one CSV header.
   */
  override def setup(
      context: Reducer[Text, Text, NullWritable, Text]#Context
  ): Unit = {

    context.write(
      NullWritable.get(),
      new Text(
        "state,window_date,most_bought_size,frequency,population_variance"
      )
    )
  }

  override def reduce(
      key: Text,
      values: Iterable[Text],
      context: Reducer[Text, Text, NullWritable, Text]#Context
  ): Unit = {

    var bestSize: String = null
    var bestFrequency = -1L
    var bestVariance =
      Double.PositiveInfinity

    val iterator =
      values.iterator()

    while (iterator.hasNext) {

      val parts =
        iterator.next()
          .toString
          .split("\\|", -1)

      if (parts.length == 4) {

        try {

          val size =
            parts(0)

          val frequency =
            parts(1).toLong

          val sumAmount =
            parts(2).toDouble

          val sumAmountSquared =
            parts(3).toDouble

          /*
           * Population variance:
           *
           * variance =
           *   sum(x^2) / N
           *   -
           *   (sum(x) / N)^2
           */
          val mean =
            sumAmount / frequency.toDouble

          val rawVariance =
            sumAmountSquared / frequency.toDouble -
            mean * mean

          /*
           * Floating-point arithmetic can occasionally
           * produce a very small negative number such as
           * -1e-12 for a mathematical variance of zero.
           */
          val variance =
            if (
              rawVariance < 0.0 &&
              math.abs(rawVariance) < 1e-9
            ) {
              0.0
            } else {
              rawVariance
            }

          /*
           * Tie-breaking:
           *
           * 1. Higher frequency
           * 2. Lower population variance
           * 3. Lexicographically smaller size
           */
          val better =
            bestSize == null ||
            frequency > bestFrequency ||
            (
              frequency == bestFrequency &&
              variance < bestVariance
            ) ||
            (
              frequency == bestFrequency &&
              java.lang.Double.compare(
                variance,
                bestVariance
              ) == 0 &&
              size.compareTo(bestSize) < 0
            )

          if (better) {
            bestSize = size
            bestFrequency = frequency
            bestVariance = variance
          }

        } catch {
          case _: Exception =>
        }
      }
    }

    if (bestSize != null) {

      val keyParts =
        key.toString.split("\\|", -1)

      if (keyParts.length == 2) {

        val state =
          keyParts(0)

        val windowDate =
          keyParts(1)

        val csvLine =
          s"$state,$windowDate,$bestSize,$bestFrequency,$bestVariance"

        context.write(
          NullWritable.get(),
          new Text(csvLine)
        )
      }
    }
  }
}


object WinnerSelectionJob {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {

      System.err.println(
        "Usage: WinnerSelectionJob " +
        "<bucket_stats_input> <output_path>"
      )

      System.exit(1)
    }

    val conf =
      new Configuration()

    val job =
      Job.getInstance(
        conf,
        "Task 1-1 Job 2 - Select Winning Size"
      )

    job.setJarByClass(
      this.getClass
    )

    job.setMapperClass(
      classOf[WinnerMapper]
    )

    job.setReducerClass(
      classOf[WinnerReducer]
    )

    job.setMapOutputKeyClass(
      classOf[Text]
    )

    job.setMapOutputValueClass(
      classOf[Text]
    )

    job.setOutputKeyClass(
      classOf[NullWritable]
    )

    job.setOutputValueClass(
      classOf[Text]
    )

    /*
     * One reducer gives one final part file,
     * which can be copied directly as a single CSV.
     */
    job.setNumReduceTasks(1)

    FileInputFormat.addInputPath(
      job,
      new Path(args(0))
    )

    FileOutputFormat.setOutputPath(
      job,
      new Path(args(1))
    )

    val success =
      job.waitForCompletion(true)

    System.exit(
      if (success) 0 else 1
    )
  }
}
