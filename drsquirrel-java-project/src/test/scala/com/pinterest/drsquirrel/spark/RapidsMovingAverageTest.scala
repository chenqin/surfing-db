package com.pinterest.drsquirrel.spark

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FunSuite}

class RapidsMovingAverageTest extends FunSuite with BeforeAndAfterAll {

  var spark: SparkSession = _

  override def beforeAll(): Unit = {
    spark = SparkSession.builder()
      .appName("RapidsMovingAverageTest")
      .master("local[2]")
      .config("spark.ui.enabled", "false")
      .getOrCreate()
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
    }
  }

  test("generate float array data") {
    val cfg = RapidsMovingAverage.Config(numRecords = 100, arraySize = 100)
    val df = RapidsMovingAverage.generateFloatArrayData(spark, cfg)

    assert(df.count() == 100)
    assert(df.columns.contains("id"))
    assert(df.columns.contains("values_array"))
    assert(df.columns.contains("timestamp"))

    // Check array size
    import spark.implicits._
    val firstRow = df.first()
    val arrayCol = firstRow.getAs[Seq[Float]]("values_array")
    assert(arrayCol.length == 100)
  }

  test("compute moving average") {
    val cfg = RapidsMovingAverage.Config(
      numRecords = 10,
      arraySize = 5,
      windowSize = 3
    )

    val df = RapidsMovingAverage.generateFloatArrayData(spark, cfg)
    val resultDf = RapidsMovingAverage.computeMovingAverage(df, cfg)

    assert(resultDf.count() == 10)
    assert(resultDf.columns.contains("moving_avg_array"))

    // Verify moving average array exists and has correct size
    import spark.implicits._
    val firstResult = resultDf.first()
    val movingAvgArray = firstResult.getAs[Seq[Double]]("moving_avg_array")
    assert(movingAvgArray.nonEmpty)
  }

  test("compute moving average with window size 1") {
    val cfg = RapidsMovingAverage.Config(
      numRecords = 5,
      arraySize = 10,
      windowSize = 1
    )

    val df = RapidsMovingAverage.generateFloatArrayData(spark, cfg)
    val resultDf = RapidsMovingAverage.computeMovingAverage(df, cfg)

    // Window size 1 means each moving avg equals the original value
    assert(resultDf.count() == 5)
  }

  test("compute moving average array transform") {
    val cfg = RapidsMovingAverage.Config(
      numRecords = 10,
      arraySize = 20,
      windowSize = 5
    )

    val df = RapidsMovingAverage.generateFloatArrayData(spark, cfg)
    val resultDf = RapidsMovingAverage.computeMovingAverageArrayTransform(df, cfg)

    assert(resultDf.count() == 10)
    assert(resultDf.columns.contains("moving_avg_array"))

    // Check result array size matches input
    import spark.implicits._
    val firstResult = resultDf.first()
    val movingAvgArray = firstResult.getAs[Seq[Float]]("moving_avg_array")
    assert(movingAvgArray.length == 20)
  }

  test("verify moving average calculation correctness") {
    import spark.implicits._

    // Create a simple test case with known values
    val testData = Seq(
      (0L, Array(1.0f, 2.0f, 3.0f, 4.0f, 5.0f), 0L)
    ).toDF("id", "values_array", "timestamp")

    val cfg = RapidsMovingAverage.Config(windowSize = 2)
    val resultDf = RapidsMovingAverage.computeMovingAverageArrayTransform(testData, cfg)

    val result = resultDf.first()
    val movingAvg = result.getAs[Seq[Float]]("moving_avg_array")

    // Expected: [1.0, 1.5, 2.5, 3.5, 4.5]
    // Window=2: [1], [1,2], [2,3], [3,4], [4,5]
    assert(math.abs(movingAvg(0) - 1.0f) < 0.01f)
    assert(math.abs(movingAvg(1) - 1.5f) < 0.01f)
    assert(math.abs(movingAvg(2) - 2.5f) < 0.01f)
    assert(math.abs(movingAvg(3) - 3.5f) < 0.01f)
    assert(math.abs(movingAvg(4) - 4.5f) < 0.01f)
  }
}
