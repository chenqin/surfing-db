#!/usr/bin/env bash
set -euo pipefail

INPUT=${1:-artifacts/thrift_batch.parquet}

if [ -z "${SPARK_HOME:-}" ]; then
  echo "SPARK_HOME not set. Please set SPARK_HOME to Spark 3.2.x installation." >&2
  exit 1
fi

if [ -z "${RAPIDS_JAR:-}" ] || [ -z "${CUDF_JAR:-}" ]; then
  echo "Set RAPIDS_JAR and CUDF_JAR env vars to the rapids-4-spark and cudf jars." >&2
  exit 1
fi

JAR=$(pwd)/drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar
if [ ! -f "$JAR" ]; then
  echo "Fat jar not found at $JAR. Build with: mvn -q -f drsquirrel-java-project/pom.xml -Darrow.version=12.0.0 -DskipTests package" >&2
  exit 1
fi

"$SPARK_HOME/bin/spark-submit" \
  --jars "$RAPIDS_JAR","$CUDF_JAR" \
  --conf spark.plugins=com.nvidia.spark.SQLPlugin \
  --conf spark.rapids.sql.enabled=true \
  --conf spark.rapids.sql.explain=NOT_ON_GPU \
  --conf spark.rapids.sql.csvReader.enabled=true \
  --class org.surfing.drsquirrel.spark.RapidsGroupBy \
  "$JAR" "$INPUT"

