# Lab 3 - MapReduce and Spark

This repository contains the source code and output files for Lab 3.

## Prerequisites

- Hadoop command line tools are available.
- Scala compiler is installed.
- The input CSV has been uploaded to HDFS.

## Task 1-1

Instructions will be added here.

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

Instructions will be added here.

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
