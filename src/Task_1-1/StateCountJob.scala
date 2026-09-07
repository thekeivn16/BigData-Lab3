import java.lang.Iterable

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat


class StateCountMapper
  extends Mapper[LongWritable, Text, Text, LongWritable] {

  private val ONE = new LongWritable(1L)

  // Column indexes in Amazon Sale Report
  private val COL_STATUS = 3
  private val COL_QTY    = 13
  private val COL_STATE  = 17

  //Parse one CSV line
  private def parseCSV(line: String): Array[String] = {
    line
      .split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -1)
      .map(_.trim.replaceAll("^\"|\"$", ""))
  }

  /*
   * Bought order:
   * - Status contains "shipped", case-insensitive
   * - Qty != 0
   */
  private def isBought(status: String, qtyString: String): Boolean = {
    try {
      val qty = qtyString.trim.toDouble

      status.trim.toLowerCase.contains("shipped") &&
      qty != 0.0
    } catch {
      case _: Exception => false
    }
  }

  override def map(
      key: LongWritable,
      value: Text,
      context: Mapper[LongWritable, Text, Text, LongWritable]#Context
  ): Unit = {

    val line = value.toString

    // Skip header
    if (line.toLowerCase.startsWith("index,")) {
      return
    }

    val cols = parseCSV(line)

    // Validate the expected 24-column schema before accessing fields
    if (cols.length == 24) {

      val status = cols(COL_STATUS)
      val qty    = cols(COL_QTY)
      val state  = cols(COL_STATE).trim.toUpperCase

      if (
        state.nonEmpty &&
        isBought(status, qty)
      ) {
        context.write(
          new Text(state),
          ONE
        )
      }
    }
  }
}


class StateCountReducer
  extends Reducer[Text, LongWritable, Text, LongWritable] {

  override def reduce(
      key: Text,
      values: Iterable[LongWritable],
      context: Reducer[Text, LongWritable, Text, LongWritable]#Context
  ): Unit = {

    var total = 0L

    val iterator = values.iterator()

    while (iterator.hasNext) {
      total += iterator.next().get()
    }

    context.write(
      key,
      new LongWritable(total)
    )
  }
}


object StateCountJob {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.err.println(
        "Usage: StateCountJob <input_path> <output_path>"
      )
      System.exit(1)
    }

    val conf = new Configuration()

    val job = Job.getInstance(
      conf,
      "Task 1-1 Job 0 - Count Bought Orders Per State"
    )

    job.setJarByClass(this.getClass)

    job.setMapperClass(classOf[StateCountMapper])

    // Same reducer can safely be used as combiner
    // because summation is associative and commutative.
    job.setCombinerClass(classOf[StateCountReducer])

    job.setReducerClass(classOf[StateCountReducer])

    job.setMapOutputKeyClass(classOf[Text])
    job.setMapOutputValueClass(classOf[LongWritable])

    job.setOutputKeyClass(classOf[Text])
    job.setOutputValueClass(classOf[LongWritable])

    // Gives one convenient output file.
    job.setNumReduceTasks(1)

    FileInputFormat.addInputPath(
      job,
      new Path(args(0))
    )

    FileOutputFormat.setOutputPath(
      job,
      new Path(args(1))
    )

    val success = job.waitForCompletion(true)

    System.exit(
      if (success) 0 else 1
    )
  }
}
