# Lab 3 - MapReduce and Spark

This repository contains the source code and output files for Lab 3.

## Prerequisites

- Hadoop command line tools are available.
- Scala compiler is installed.
- The input CSV has been uploaded to HDFS.

## Task 1-1

Dynamic sliding-window computation using Hadoop MapReduce. The solution consists
of three jobs:

- `StateCountJob.scala`: counts bought records per state.
- `BucketAggregationJob.scala`: maps bought records to window buckets and aggregates statistics.
- `WinnerSelectionJob.scala`: selects the winning size using frequency, variance, and lexical tie-breaking.

### Compile

```bash
cd ~/Lab3/Task_1-1

scalac -classpath "$(hadoop classpath)" \
  -d build/classes-state src/StateCountJob.scala

scalac -classpath "$(hadoop classpath)" \
  -d build/classes-bucket src/BucketAggregationJob.scala

scalac -classpath "$(hadoop classpath)" \
  -d build/classes-winner src/WinnerSelectionJob.scala
```

### Run

```bash
# Job 0
hadoop jar build/StateCountJob-fat.jar StateCountJob \
  /lab3/task1_1/input \
  /lab3/task1_1/state_counts

# Job 1
hadoop jar build/BucketAggregationJob-fat.jar BucketAggregationJob \
  /lab3/task1_1/input \
  /lab3/task1_1/state_counts/part-r-00000 \
  /lab3/task1_1/bucket_stats

# Job 2
hadoop jar build/WinnerSelectionJob-fat.jar WinnerSelectionJob \
  /lab3/task1_1/bucket_stats \
  /lab3/task1_1/final
```

## Task 1-2

### Compile

From the project directory:

```bash
mkdir -p build

scalac \
  -classpath "$(hadoop classpath)" \
  -d build \
  Task_1-2.scala
```

### Build Fat JAR

```bash
SCALA_LIB=$(find /usr/share -name "scala-library*.jar" 2>/dev/null | head -1)

mkdir -p fatjar_tmp
cp -r build/* fatjar_tmp/

cd fatjar_tmp
jar xf "$SCALA_LIB"
jar cf ../task12-fat.jar .
cd ..
```

### Run

Run the job:

```bash
hadoop jar task12-fat.jar Task12 \
  /user/<username>/lab3/input/asr.csv \
  /user/<username>/lab3/task12_work \
  file://$(pwd)/Task_1-2.csv
```

The final CSV is written to:

```text
Task_1-2.csv
```

## Task 2-1

Percentage of Cancelled + Standard orders by city that have at least three temporally valid promotions and an amount below the associated state's Merchant/Shipped average. The input is read from HDFS and the final submission is exported as one normal-filesystem Parquet file.

### Prerequisites

- Spark 4.2.0 with Scala 2.13 and JDK 17.
- HDFS is configured at `hdfs://localhost:9000`.
- The input CSV exists at:

```text
/user/vandiemmy/lab3/input/Amazon Sale Report.csv
```

Start HDFS and YARN, then verify the input:

```bash
start-dfs.sh
start-yarn.sh
jps

hdfs dfs -ls -h \
  '/user/vandiemmy/lab3/input/Amazon Sale Report.csv'
```

```bash
cd src/Task_2-1

TASK21_SPARK_HOME="$(dirname "$(dirname \
  "$(readlink -f "$(command -v spark-submit)")")")"
TASK21_BUILD_DIR="$(mktemp -d /tmp/task21-yarn.XXXXXX)"

mkdir -p "$TASK21_BUILD_DIR/classes"

java \
  -cp "$TASK21_SPARK_HOME/jars/*" \
  scala.tools.nsc.Main \
  -classpath "$TASK21_SPARK_HOME/jars/*" \
  -d "$TASK21_BUILD_DIR/classes" \
  Task21.scala

jar cf "$TASK21_BUILD_DIR/Task21.jar" \
  -C "$TASK21_BUILD_DIR/classes" .
```

### Run separate YARN applications

Each application evaluates and writes the pipeline once. Five separate
applications are required because YARN reports `memory-seconds` and
`vcore-seconds` per application.

Run the following in the same terminal used to create the temporary JAR:

```bash
set -eo pipefail
mkdir -p /home/vandiemmy/lab3/task21-yarn-benchmark

for TASK21_RUN in 1 2 3 4 5; do
  TASK21_SUBMIT_LOG="/home/vandiemmy/lab3/task21-yarn-benchmark/submit-${TASK21_RUN}.log"

  "$TASK21_SPARK_HOME/bin/spark-submit" \
    --master yarn \
    --deploy-mode cluster \
    --conf spark.yarn.submit.waitAppCompletion=true \
    --conf spark.yarn.appMasterEnv.JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    --conf spark.executorEnv.JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    --class Task21 \
    "$TASK21_BUILD_DIR/Task21.jar" \
    'hdfs://localhost:9000/user/vandiemmy/lab3/input/Amazon Sale Report.csv' \
    'hdfs://localhost:9000/user/vandiemmy/lab3/output/Task_2-1_temp' \
    "$TASK21_RUN" \
    2>&1 | tee "$TASK21_SUBMIT_LOG"

  TASK21_APP_ID="$(grep -oE 'application_[0-9]+_[0-9]+' \
    "$TASK21_SUBMIT_LOG" | tail -n 1)"

  yarn application -status "$TASK21_APP_ID" 2>&1 | \
    tee "/home/vandiemmy/lab3/task21-yarn-benchmark/status-${TASK21_RUN}.txt"
done
```

To inspect the relevant physical-plan operators:

```bash
TASK21_LAST_APP_ID="$(grep -oE 'application_[0-9]+_[0-9]+' \
  /home/vandiemmy/lab3/task21-yarn-benchmark/submit-5.log | tail -n 1)"

TASK21_DRIVER_LOG="$(find \
  "$HADOOP_HOME/logs/userlogs/$TASK21_LAST_APP_ID" \
  -path '*/container_*_000001/stdout' -print -quit)"

sed -n \
  '/== Physical Plan ==/,/========== YARN BENCHMARK RUN ==========/p' \
  "$TASK21_DRIVER_LOG" |
grep -E 'BroadcastHashJoin|BroadcastExchange|Exchange hashpartitioning'
```

### Verify the HDFS result

Start PySpark:

```bash
pyspark --master 'local[*]'
```

Then run:

```python
from pyspark.sql import functions as F

result = spark.read.parquet(
    "hdfs://localhost:9000/user/vandiemmy/lab3/output/Task_2-1_temp"
)

result.printSchema()
print("Number of cities:", result.count())
result.agg(
    F.min("percentage").alias("minimum_percentage"),
    F.max("percentage").alias("maximum_percentage")
).show()
result.show(10, truncate=False)
```

Expected validation:

```text
city: string
percentage: double
percentage range: 0.0
```

### Export the single submission file

Copy the one Parquet part-file to a path without spaces first:

```bash
mkdir -p /home/vandiemmy/lab3/result

hdfs dfs -get -f \
  '/user/vandiemmy/lab3/output/Task_2-1_temp/part-*.parquet' \
  /home/vandiemmy/lab3/result/Task_2-1.parquet
```

## Task 2-2

Per-SKU-month P80/P90 promotion-count thresholds and the population standard
deviation of `Amount` over the qualifying orders, computed with Spark's
`percentile_approx` and with a self-implemented exact percentile. Exports one
Parquet file.

Requires Spark 3.5.4, JDK 17, sbt, and the Lab 1 Hadoop cluster running with
the input CSV at `hdfs://namenode:9000/lab3/task2-2/input/asr.csv`. The Docker
Compose file and Hadoop configuration are in the Drive folder linked from
`docs/drive_link.txt`.

### Set up and build

Create the sbt layout, write the build files, then compile:

```bash
mkdir -p ~/task-2-2/src/main/scala ~/task-2-2/project
cp src/Task_2-2/*.scala ~/task-2-2/src/main/scala/
cd ~/task-2-2

cat > build.sbt <<'EOF'
name := "task-2-2"
version := "0.1.0"
scalaVersion := "2.12.18"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.4" % "provided"
EOF

echo 'sbt.version=1.13.0' > project/build.properties

sbt package
```

Produces `target/scala-2.12/task-2-2_2.12-0.1.0.jar`. Spark is `provided`, so
run it with `spark-submit`, not `java -jar`.

### Run on YARN

Point Spark at the cluster and resolve the Docker bridge gateway:

```bash
export HADOOP_CONF_DIR="$HOME/hadoop-cluster-config/conf"
export DRIVER_IP=$(docker network inspect hadoop-cluster-config_hadoopnet \
  -f '{{(index .IPAM.Config 0).Gateway}}')
mkdir -p evidence
```

Submit (arguments: input CSV URI, output directory URI):

```bash
spark-submit \
  --class Main \
  --master yarn \
  --deploy-mode client \
  --num-executors 2 \
  --executor-cores 1 \
  --executor-memory 1g \
  --driver-memory 1g \
  --conf spark.dynamicAllocation.enabled=false \
  --conf spark.driver.host="$DRIVER_IP" \
  --conf spark.driver.bindAddress=0.0.0.0 \
  target/scala-2.12/task-2-2_2.12-0.1.0.jar \
  hdfs://namenode:9000/lab3/task2-2/input/asr.csv \
  hdfs://namenode:9000/lab3/task2-2/output/result-yarn \
  2>&1 | tee evidence/run-yarn.log
```

### Retrieve and verify the result

Copy the part-file out and name it `Task_2-2.parquet`:

```bash
docker exec namenode /opt/hadoop/bin/hdfs dfs -get -f \
  '/lab3/task2-2/output/result-yarn/part-*.parquet' /tmp/Task_2-2.parquet
docker cp namenode:/tmp/Task_2-2.parquet ./Task_2-2.parquet
```

Read it back outside HDFS:

```bash
spark-submit \
  --class VerifyOutput \
  --master "local[2]" \
  target/scala-2.12/task-2-2_2.12-0.1.0.jar \
  ./Task_2-2.parquet
```

Expected:

```text
+------+-----+
|method|count|
+------+-----+
|exact |16486|
|approx|16486|
+------+-----+

Verified rows: 32972
PARQUET_READBACK_OK
```
