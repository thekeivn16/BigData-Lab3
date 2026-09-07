import java.io.{BufferedReader, BufferedWriter, InputStreamReader, OutputStreamWriter, IOException}
import java.nio.charset.StandardCharsets
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Locale

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io._
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat

import scala.collection.JavaConverters._
import scala.collection.mutable

object Task12 {

  // 0-based column indexes.
  private val DATE_IDX  = 2
  private val STYLE_IDX = 7
  private val SKU_IDX   = 8
  private val SIZE_IDX  = 10
  private val STATE_IDX = 17

  private val EXPECTED_COLUMNS = 24

  private val INPUT_DATE_FORMAT =
    DateTimeFormatter.ofPattern("MM-dd-yy", Locale.ROOT)

  // Counters help check skipped/bad rows in Hadoop logs.
  object Counters {
    val GROUP = "TASK12"
    val HEADER_ROWS = "HEADER_ROWS"
    val BAD_CSV_ROWS = "BAD_CSV_ROWS"
    val BAD_DATE_ROWS = "BAD_DATE_ROWS"
    val MISSING_GROUP_FIELDS = "MISSING_GROUP_FIELDS"
    val QUALIFYING_XXL_ROWS = "QUALIFYING_XXL_ROWS"
    val QUALIFIED_STYLE_ROWS = "QUALIFIED_STYLE_ROWS"
  }

  // Utility functions

  // Quote-aware CSV parser.
  def parseCsvLine(line: String): Array[String] = {
    val fields = mutable.ArrayBuffer.empty[String]
    val current = new StringBuilder

    var insideQuotes = false
    var i = 0

    while (i < line.length) {
      val ch = line.charAt(i)

      if (ch == '"') {
        if (
          insideQuotes &&
          i + 1 < line.length &&
          line.charAt(i + 1) == '"'
        ) {
          current.append('"')
          i += 1
        } else {
          insideQuotes = !insideQuotes
        }
      } else if (ch == ',' && !insideQuotes) {
        fields += current.toString()
        current.clear()
      } else {
        current.append(ch)
      }

      i += 1
    }

    fields += current.toString()
    fields.toArray
  }

  def isHeader(cols: Array[String]): Boolean =
    cols.nonEmpty && cols(0).trim.equalsIgnoreCase("index")

  def normalizeState(raw: String): String =
    Option(raw).getOrElse("").trim.replaceAll("\\s+", " ").toUpperCase(Locale.ROOT)

  def normalizeStyle(raw: String): String =
    Option(raw).getOrElse("").trim.toUpperCase(Locale.ROOT)

  def normalizeSku(raw: String): String =
    Option(raw).getOrElse("").trim.toUpperCase(Locale.ROOT)

  // Converts MM-dd-yy to yyyy-MM.
  def extractMonth(raw: String): Option[String] = {
    try {
      val date = LocalDate.parse(raw.trim, INPUT_DATE_FORMAT)
      Some(f"${date.getYear}%04d-${date.getMonthValue}%02d")
    } catch {
      case _: Exception => None
    }
  }

  // Ranks sizes so XXL and above can be checked.
  def sizeRank(raw: String): Option[Int] = {
    val size = Option(raw).getOrElse("").trim.replaceAll("\\s+", "").toUpperCase(Locale.ROOT)

    size match {
      case "XS"  => Some(1)
      case "S"   => Some(2)
      case "M"   => Some(3)
      case "L"   => Some(4)
      case "XL"  => Some(5)
      case "XXL" => Some(6)

      case s if s.matches("[2-9][0-9]*XL") =>
        val n = s.stripSuffix("XL").toInt
        Some(n + 4)

      case s if s.matches("X{2,}L") =>
        val numberOfX = s.length - 1
        Some(numberOfX + 4)

      case _ => None
    }
  }

  def isAtLeastXXL(raw: String): Boolean =
    sizeRank(raw).exists(_ >= 6)

  // Job 1: Find styles that have Size >= XXL anywhere in the full dataset.

  class GlobalValidStyleMapper
      extends Mapper[LongWritable, Text, Text, NullWritable] {

    private val outKey = new Text()

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[LongWritable, Text, Text, NullWritable]#Context
    ): Unit = {
      val cols = parseCsvLine(value.toString)

      if (cols.length != EXPECTED_COLUMNS) {
        context.getCounter(Counters.GROUP, Counters.BAD_CSV_ROWS).increment(1)
        return
      }

      if (isHeader(cols)) {
        context.getCounter(Counters.GROUP, Counters.HEADER_ROWS).increment(1)
        return
      }

      val style = normalizeStyle(cols(STYLE_IDX))
      val size = cols(SIZE_IDX)

      if (style.nonEmpty && isAtLeastXXL(size)) {
        outKey.set(style)
        context.write(outKey, NullWritable.get())

        context
          .getCounter(Counters.GROUP, Counters.QUALIFYING_XXL_ROWS)
          .increment(1)
      }
    }
  }

  // Deduplicates style keys.
  class GlobalValidStyleReducer
      extends Reducer[Text, NullWritable, Text, NullWritable] {

    override def reduce(
        key: Text,
        values: java.lang.Iterable[NullWritable],
        context: Reducer[Text, NullWritable, Text, NullWritable]#Context
    ): Unit = {
      context.write(key, NullWritable.get())
    }
  }

  // Job 2: Compute variety per (month, state, style)

  class VarietyMapper
      extends Mapper[LongWritable, Text, Text, Text] {

    private val globallyValidStyles = mutable.HashSet.empty[String]

    private val outKey = new Text()
    private val outValue = new Text()

    // Loads globally-qualified styles from Job 1.
    override def setup(
        context: Mapper[LongWritable, Text, Text, Text]#Context
    ): Unit = {
      val cacheFiles = context.getCacheFiles

      if (cacheFiles != null) {
        cacheFiles.foreach { uri =>
          val cachePath = new Path(uri)
          val cacheFs = cachePath.getFileSystem(context.getConfiguration)

          val reader = new BufferedReader(
            new InputStreamReader(
              cacheFs.open(cachePath),
              StandardCharsets.UTF_8
            )
          )

          try {
            var line = reader.readLine()

            while (line != null) {
              val style = normalizeStyle(line)
              if (style.nonEmpty) {
                globallyValidStyles += style
              }
              line = reader.readLine()
            }
          } finally {
            reader.close()
          }
        }
      }
    }

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[LongWritable, Text, Text, Text]#Context
    ): Unit = {
      val cols = parseCsvLine(value.toString)

      if (cols.length != EXPECTED_COLUMNS) {
        context.getCounter(Counters.GROUP, Counters.BAD_CSV_ROWS).increment(1)
        return
      }

      if (isHeader(cols)) {
        context.getCounter(Counters.GROUP, Counters.HEADER_ROWS).increment(1)
        return
      }

      val style = normalizeStyle(cols(STYLE_IDX))

      // Keep only styles that qualify globally.
      if (!globallyValidStyles.contains(style)) {
        return
      }

      val monthOpt = extractMonth(cols(DATE_IDX))

      if (monthOpt.isEmpty) {
        context.getCounter(Counters.GROUP, Counters.BAD_DATE_ROWS).increment(1)
        return
      }

      val state = normalizeState(cols(STATE_IDX))
      val sku = normalizeSku(cols(SKU_IDX))

      if (state.isEmpty || style.isEmpty || sku.isEmpty) {
        context
          .getCounter(Counters.GROUP, Counters.MISSING_GROUP_FIELDS)
          .increment(1)
        return
      }

      val month = monthOpt.get

      // Keep style in the key so reducer can count distinct SKU per style.
      outKey.set(s"$month|$state|$style")
      outValue.set(sku)

      context.write(outKey, outValue)

      context
        .getCounter(Counters.GROUP, Counters.QUALIFIED_STYLE_ROWS)
        .increment(1)
    }
  }

  // Removes duplicate SKU values before shuffle.
  class DistinctSkuCombiner
      extends Reducer[Text, Text, Text, Text] {

    private val outValue = new Text()

    override def reduce(
        key: Text,
        values: java.lang.Iterable[Text],
        context: Reducer[Text, Text, Text, Text]#Context
    ): Unit = {
      val uniqueSkus = mutable.HashSet.empty[String]

      values.asScala.foreach { value =>
        uniqueSkus += value.toString
      }

      uniqueSkus.foreach { sku =>
        outValue.set(sku)
        context.write(key, outValue)
      }
    }
  }

  class VarietyReducer
      extends Reducer[Text, Text, Text, IntWritable] {

    private val outKey = new Text()
    private val outValue = new IntWritable()

    override def reduce(
        key: Text,
        values: java.lang.Iterable[Text],
        context: Reducer[Text, Text, Text, IntWritable]#Context
    ): Unit = {
      val uniqueSkus = mutable.HashSet.empty[String]

      values.asScala.foreach { sku =>
        uniqueSkus += sku.toString
      }

      val parts = key.toString.split("\\|", -1)

      if (parts.length == 3) {
        val month = parts(0)
        val state = parts(1)

        val variety = uniqueSkus.size

        // Job 3 groups variety values by (month, state).
        outKey.set(s"$month|$state")
        outValue.set(variety)

        context.write(outKey, outValue)
      }
    }
  }

  // Job 3: Median variety per (month, state)

  class MedianMapper
      extends Mapper[LongWritable, Text, Text, IntWritable] {

    private val outKey = new Text()
    private val outValue = new IntWritable()

    override def map(
        key: LongWritable,
        value: Text,
        context: Mapper[LongWritable, Text, Text, IntWritable]#Context
    ): Unit = {
      // Job 2 output: month|state<TAB>variety
      val line = value.toString.trim

      if (line.nonEmpty) {
        val parts = line.split("\t", 2)

        if (parts.length == 2) {
          try {
            outKey.set(parts(0))
            outValue.set(parts(1).trim.toInt)
            context.write(outKey, outValue)
          } catch {
            case _: NumberFormatException =>
          }
        }
      }
    }
  }

  class MedianReducer
      extends Reducer[Text, IntWritable, Text, DoubleWritable] {

    private val outValue = new DoubleWritable()

    override def reduce(
        key: Text,
        values: java.lang.Iterable[IntWritable],
        context: Reducer[Text, IntWritable, Text, DoubleWritable]#Context
    ): Unit = {
      // Each value is one style's variety in the same (month, state).
      val sorted = values.asScala.map(_.get()).toArray.sorted
      val n = sorted.length

      if (n == 0) {
        return
      }

      val median =
        if (n % 2 == 1) {
          sorted(n / 2).toDouble
        } else {
          (sorted(n / 2 - 1).toDouble + sorted(n / 2).toDouble) / 2.0
        }

      outValue.set(median)
      context.write(key, outValue)
    }
  }

  // Final output helper

  // Converts Job 3 output to one CSV file.
  def writeSingleCsv(
      conf: Configuration,
      job3OutputDir: Path,
      finalCsvPath: Path
  ): Unit = {

    val job3Fs = job3OutputDir.getFileSystem(conf)
    // Use file:/// in finalCsvPath when the final CSV must be on local filesystem.
    val finalFs = finalCsvPath.getFileSystem(conf)

    val partFiles = job3Fs
      .listStatus(job3OutputDir)
      .filter(status =>
        status.isFile && status.getPath.getName.startsWith("part-r-")
      )
      .map(_.getPath)
      .sortBy(_.getName)

    if (partFiles.isEmpty) {
      throw new IOException(
        s"No reducer part file found in ${job3OutputDir.toString}"
      )
    }

    if (finalFs.exists(finalCsvPath)) {
      finalFs.delete(finalCsvPath, true)
    }

    val parent = finalCsvPath.getParent
    if (parent != null && !finalFs.exists(parent)) {
      finalFs.mkdirs(parent)
    }

    val writer = new BufferedWriter(
      new OutputStreamWriter(
        finalFs.create(finalCsvPath, true),
        StandardCharsets.UTF_8
      )
    )

    try {
      writer.write("month,state,median_variety")
      writer.newLine()

      partFiles.foreach { part =>
        val reader = new BufferedReader(
          new InputStreamReader(
            job3Fs.open(part),
            StandardCharsets.UTF_8
          )
        )

        try {
          var line = reader.readLine()

          while (line != null) {
            // Expected: yyyy-MM|STATE<TAB>median
            val tabParts = line.split("\t", 2)

            if (tabParts.length == 2) {
              val keyParts = tabParts(0).split("\\|", 2)

              if (keyParts.length == 2) {
                val month = keyParts(0)
                val state = keyParts(1)
                val median = tabParts(1)

                writer.write(s"$month,$state,$median")
                writer.newLine()
              }
            }

            line = reader.readLine()
          }
        } finally {
          reader.close()
        }
      }
    } finally {
      writer.close()
    }
  }

  // Main driver

  def main(args: Array[String]): Unit = {

    if (args.length != 3) {
      System.err.println(
        "Usage: Task12 <input_csv> <work_dir> <final_csv>"
      )
      System.err.println(
        "Example: hadoop jar task12.jar Task12 " +
          "/data/asr.csv /tmp/task12_work " +
          "file:///home/user/Task_1-2.csv"
      )
      System.exit(1)
    }

    val inputPath = new Path(args(0))
    val workDir = new Path(args(1))
    val finalCsvPath = new Path(args(2))

    val conf = new Configuration()

    val job1Output = new Path(workDir, "job1_global_valid_styles")
    val job2Output = new Path(workDir, "job2_variety")
    val job3Output = new Path(workDir, "job3_median")

    // Remove old temporary outputs.
    Seq(job1Output, job2Output, job3Output).foreach { path =>
      val fs = path.getFileSystem(conf)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }

    // Job 1: Global size qualification
    val job1 = Job.getInstance(
      new Configuration(conf),
      "Task 1-2 Job 1 - Global XXL style qualification"
    )

    job1.setJarByClass(Task12.getClass)

    job1.setMapperClass(classOf[GlobalValidStyleMapper])
    job1.setCombinerClass(classOf[GlobalValidStyleReducer])
    job1.setReducerClass(classOf[GlobalValidStyleReducer])

    job1.setMapOutputKeyClass(classOf[Text])
    job1.setMapOutputValueClass(classOf[NullWritable])

    job1.setOutputKeyClass(classOf[Text])
    job1.setOutputValueClass(classOf[NullWritable])

    // One reducer creates a single cache file for Job 2.
    job1.setNumReduceTasks(1)

    FileInputFormat.addInputPath(job1, inputPath)
    FileOutputFormat.setOutputPath(job1, job1Output)

    if (!job1.waitForCompletion(true)) {
      System.err.println("Task 1-2: Job 1 failed.")
      System.exit(2)
    }

    val job1Fs = job1Output.getFileSystem(conf)

    val validStylePartFiles = job1Fs
      .listStatus(job1Output)
      .filter(status =>
        status.isFile && status.getPath.getName.startsWith("part-r-")
      )
      .map(_.getPath)

    if (validStylePartFiles.isEmpty) {
      throw new IOException(
        "Job 1 completed but produced no valid-style part file."
      )
    }

    // Job 2: Variety per (month, state, style)
    val job2 = Job.getInstance(
      new Configuration(conf),
      "Task 1-2 Job 2 - Variety per month-state-style"
    )

    job2.setJarByClass(Task12.getClass)

    job2.setMapperClass(classOf[VarietyMapper])
    job2.setCombinerClass(classOf[DistinctSkuCombiner])
    job2.setReducerClass(classOf[VarietyReducer])

    job2.setMapOutputKeyClass(classOf[Text])
    job2.setMapOutputValueClass(classOf[Text])

    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])

    // Pass Job 1 result to Job 2 through distributed cache.
    validStylePartFiles.foreach { path =>
      job2.addCacheFile(path.toUri)
    }

    FileInputFormat.addInputPath(job2, inputPath)
    FileOutputFormat.setOutputPath(job2, job2Output)

    if (!job2.waitForCompletion(true)) {
      System.err.println("Task 1-2: Job 2 failed.")
      System.exit(3)
    }

    // Job 3: Median variety per (month, state)
    val job3 = Job.getInstance(
      new Configuration(conf),
      "Task 1-2 Job 3 - Median variety per month-state"
    )

    job3.setJarByClass(Task12.getClass)

    job3.setMapperClass(classOf[MedianMapper])
    job3.setReducerClass(classOf[MedianReducer])

    job3.setMapOutputKeyClass(classOf[Text])
    job3.setMapOutputValueClass(classOf[IntWritable])

    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[DoubleWritable])

    // One reducer makes final CSV export straightforward.
    job3.setNumReduceTasks(1)

    FileInputFormat.addInputPath(job3, job2Output)
    FileOutputFormat.setOutputPath(job3, job3Output)

    if (!job3.waitForCompletion(true)) {
      System.err.println("Task 1-2: Job 3 failed.")
      System.exit(4)
    }

    // Create final CSV.
    writeSingleCsv(conf, job3Output, finalCsvPath)

    // Delete temporary outputs after CSV creation.
    Seq(job1Output, job2Output, job3Output).foreach { path =>
      val fs = path.getFileSystem(conf)
      if (fs.exists(path)) {
        fs.delete(path, true)
      }
    }

    println(s"Task 1-2 completed successfully. Final CSV: ${finalCsvPath.toString}")
  }
}
