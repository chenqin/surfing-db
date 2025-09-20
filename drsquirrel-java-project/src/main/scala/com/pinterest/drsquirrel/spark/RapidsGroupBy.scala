package com.pinterest.drsquirrel.spark

import org.apache.spark.sql.SparkSession

object RapidsGroupBy {
  def main(args: Array[String]): Unit = {
    val input = if (args.length > 0) args(0) else "artifacts/thrift_batch.csv"
    val spark = SparkSession.builder()
      .appName("RapidsGroupBy")
      .getOrCreate()
    try {
      val df = spark.read.option("header","true").option("inferSchema","true").csv(input)
      val res = df.groupBy("service_name").count()
      res.show(false)
    } finally {
      spark.stop()
    }
  }
}

