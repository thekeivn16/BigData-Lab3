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

Instructions will be added here.
