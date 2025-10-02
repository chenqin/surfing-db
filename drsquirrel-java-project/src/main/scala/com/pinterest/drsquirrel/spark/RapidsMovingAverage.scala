package com.pinterest.drsquirrel.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * Spark RAPIDS example demonstrating GPU-accelerated moving average computation
 * on float arrays using NVIDIA CUDA and Apache Spark with RAPIDS Accelerator.
 *
 * This example:
 * 1. Generates sample data with 100-element float arrays
 * 2. Computes moving averages on GPU using RAPIDS
 * 3. Demonstrates GPU memory optimization for large-scale processing
 *
 * Usage:
 *   spark-submit --master local[*] \
 *     --conf spark.rapids.sql.enabled=true \
 *     --conf spark.plugins=com.nvidia.spark.SQLPlugin \
 *     --conf spark.executor.resource.gpu.amount=1 \
 *     --conf spark.task.resource.gpu.amount=0.25 \
 *     --class com.pinterest.drsquirrel.spark.RapidsMovingAverage \
 *     drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *     --num-records 1000000 --window-size 5
 */
object RapidsMovingAverage {

  case class Config(
    numRecords: Int = 1000000,
    windowSize: Int = 5,
    arraySize: Int = 100,
    outputPath: String = "artifacts/moving_avg_output.parquet",
    printSample: Boolean = true
  )

  def main(args: Array[String]): Unit = {
    val cfg = parseArgs(args.toList, Config())

    val spark = SparkSession.builder()
      .appName("RAPIDS Moving Average")
      .config("spark.rapids.sql.enabled", "true")
      .config("spark.plugins", "com.nvidia.spark.SQLPlugin")
      .config("spark.rapids.sql.explain", "ALL")
      .getOrCreate()

    try {
      println(s"""
        |==========================================================
        |RAPIDS Moving Average GPU Acceleration Demo
        |==========================================================
        |Records: ${cfg.numRecords}
        |Array size: ${cfg.arraySize} floats per record
        |Window size: ${cfg.windowSize}
        |Output: ${cfg.outputPath}
        |==========================================================
        |""".stripMargin)

      // Generate sample data with float arrays
      val df = generateFloatArrayData(spark, cfg)

      // Compute moving average on GPU
      val resultDf = computeMovingAverage(df, cfg)

      // Show sample results
      if (cfg.printSample) {
        println("\nSample Results (first 10 rows):")
        resultDf.select("id", "values_array", "moving_avg_array").show(10, truncate = false)
      }

      // Write results to Parquet
      resultDf.write.mode("overwrite").parquet(cfg.outputPath)
      println(s"\nResults written to: ${cfg.outputPath}")

      // Print statistics
      printStatistics(resultDf)

    } finally {
      spark.stop()
    }
  }

  /**
   * Generate sample DataFrame with float arrays
   */
  def generateFloatArrayData(spark: SparkSession, cfg: Config): DataFrame = {
    import spark.implicits._

    println(s"Generating ${cfg.numRecords} records with ${cfg.arraySize}-element float arrays...")

    // Create UDF to generate random float arrays
    val generateFloatArray = udf((seed: Long) => {
      val rnd = new scala.util.Random(seed)
      (0 until cfg.arraySize).map(_ => rnd.nextFloat() * 100.0f).toArray
    })

    // Generate sequential IDs and convert to float arrays
    val df = spark.range(0, cfg.numRecords)
      .withColumn("values_array", generateFloatArray(col("id")))
      .withColumn("timestamp", (col("id") * 1000).cast(LongType))

    println(s"Generated ${df.count()} records")
    df
  }

  /**
   * Compute moving average using window functions (GPU-accelerated via RAPIDS)
   */
  def computeMovingAverage(df: DataFrame, cfg: Config): DataFrame = {
    import org.apache.spark.sql.expressions.Window

    println(s"Computing moving average with window size ${cfg.windowSize}...")

    // Explode array to individual elements for window computation
    val explodedDf = df
      .withColumn("array_element", explode(col("values_array")))
      .withColumn("element_index", monotonically_increasing_id())

    // Define window specification for moving average
    val windowSpec = Window
      .orderBy("id", "element_index")
      .rowsBetween(-cfg.windowSize + 1, 0)

    // Compute moving average (RAPIDS will execute this on GPU)
    val movingAvgDf = explodedDf
      .withColumn("moving_avg", avg(col("array_element")).over(windowSpec))
      .withColumn("moving_sum", sum(col("array_element")).over(windowSpec))
      .withColumn("moving_count", count(col("array_element")).over(windowSpec))

    // Collect back into arrays
    val resultDf = movingAvgDf
      .groupBy("id", "values_array", "timestamp")
      .agg(
        collect_list("moving_avg").alias("moving_avg_array"),
        collect_list("moving_sum").alias("moving_sum_array"),
        collect_list("moving_count").alias("moving_count_array")
      )
      .orderBy("id")

    resultDf
  }

  /**
   * Alternative implementation using array transformations (RAPIDS GPU-accelerated)
   */
  def computeMovingAverageArrayTransform(df: DataFrame, cfg: Config): DataFrame = {
    val windowSize = cfg.windowSize

    // UDF for computing moving average on array
    val movingAvgUdf = udf((arr: Seq[Float]) => {
      arr.zipWithIndex.map { case (_, idx) =>
        val start = math.max(0, idx - windowSize + 1)
        val end = idx + 1
        val window = arr.slice(start, end)
        window.sum / window.length.toFloat
      }
    })

    df.withColumn("moving_avg_array", movingAvgUdf(col("values_array")))
  }

  /**
   * Print execution statistics
   */
  def printStatistics(df: DataFrame): Unit = {
    println("\n==========================================================")
    println("Execution Statistics:")
    println("==========================================================")

    // Calculate statistics on moving averages
    val stats = df.agg(
      count("id").alias("total_records"),
      avg(size(col("moving_avg_array"))).alias("avg_array_size")
    ).collect()(0)

    println(s"Total records processed: ${stats.getLong(0)}")
    println(s"Average array size: ${stats.getDouble(1)}")
    println("==========================================================\n")
  }

  /**
   * Parse command-line arguments
   */
  private def parseArgs(args: List[String], c: Config): Config = args match {
    case Nil => c
    case "--num-records" :: v :: tail => parseArgs(tail, c.copy(numRecords = v.toInt))
    case "--window-size" :: v :: tail => parseArgs(tail, c.copy(windowSize = v.toInt))
    case "--array-size" :: v :: tail => parseArgs(tail, c.copy(arraySize = v.toInt))
    case "--output" :: v :: tail => parseArgs(tail, c.copy(outputPath = v))
    case "--no-print" :: tail => parseArgs(tail, c.copy(printSample = false))
    case other :: _ => throw new IllegalArgumentException(s"Unknown arg: $other")
  }
}
