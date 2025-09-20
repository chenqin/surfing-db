package com.pinterest.drsquirrel.spark

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object RapidsGroupBy {
  case class Config(
    inputPath: String = "artifacts/thrift_batch.parquet",
    inputMode: String = "parquet", // parquet|csv|thrift
    inFormat: String = "lines-base64", // for thrift: lines-base64|raw-len
    decoder: String = "java", // java|jni
    thriftPath: String = null,
    structName: String = null
  )

  def main(args: Array[String]): Unit = {
    val cfg = parseArgs(args.toList, Config())
    val spark = SparkSession.builder().appName("RapidsGroupBy").getOrCreate()
    try {
      val df = cfg.inputMode.toLowerCase match {
        case "parquet" => spark.read.parquet(cfg.inputPath)
        case "csv" => spark.read.option("header","true").option("inferSchema","true").csv(cfg.inputPath)
        case "thrift" => decodeThriftToDf(spark, cfg)
        case other => throw new IllegalArgumentException(s"Unknown input-mode: $other")
      }
      val res = df.groupBy("service_name").count()
      res.show(false)
    } finally {
      spark.stop()
    }
  }

  private def parseArgs(args: List[String], c: Config): Config = args match {
    case Nil => c
    case "--input-mode" :: v :: tail => parseArgs(tail, c.copy(inputMode = v))
    case "--input" :: v :: tail => parseArgs(tail, c.copy(inputPath = v))
    case "--in-format" :: v :: tail => parseArgs(tail, c.copy(inFormat = v))
    case "--decoder" :: v :: tail => parseArgs(tail, c.copy(decoder = v))
    case "--thrift-path" :: v :: tail => parseArgs(tail, c.copy(thriftPath = v))
    case "--struct-name" :: v :: tail => parseArgs(tail, c.copy(structName = v))
    case other :: _ => throw new IllegalArgumentException(s"Unknown arg: $other")
  }

  private[spark] def convertThriftPayloads(cfg: Config, alloc: org.apache.arrow.memory.BufferAllocator) = {
    val payloads: Array[Array[Byte]] = cfg.inFormat.toLowerCase match {
      case "lines-base64" =>
        val src = scala.io.Source.fromFile(cfg.inputPath)
        try src.getLines().toArray.filter(_.nonEmpty).map(java.util.Base64.getDecoder.decode)
        finally src.close()
      case "raw-len" =>
        val in = new java.io.DataInputStream(new java.io.BufferedInputStream(new java.io.FileInputStream(cfg.inputPath)))
        val buf = scala.collection.mutable.ArrayBuffer.empty[Array[Byte]]
        try {
          while (in.available() > 0) {
            val len = in.readInt()
            val arr = new Array[Byte](len)
            in.readFully(arr)
            buf += arr
          }
        } finally in.close()
        buf.toArray
      case other => throw new IllegalArgumentException(s"Unknown in-format: $other")
    }

    def convertViaJava(): org.apache.arrow.vector.VectorSchemaRoot = {
      val conv = new org.apache.thrift.ext.GenericThriftToArrowConverter()
      import scala.collection.JavaConverters._
      conv.convert(payloads.toSeq.asJava, classOf[com.pinterest.mabs_metrics.thrift.MabsMetrics], alloc)
    }

    val useNative = cfg.decoder.equalsIgnoreCase("jni") && cfg.thriftPath != null && cfg.structName != null
    if (useNative) {
      try {
        com.pinterest.drsquirrel.jni.NativeThriftDecoder.convert(alloc, payloads, cfg.thriftPath, cfg.structName)
      } catch {
        case _: Throwable => convertViaJava()
      }
    } else {
      convertViaJava()
    }
  }

  private def decodeThriftToDf(spark: SparkSession, cfg: Config) = {
    val alloc = new org.apache.arrow.memory.RootAllocator()
    val root = convertThriftPayloads(cfg, alloc)
    try {
      val svc = root.getVector("service_name").asInstanceOf[org.apache.arrow.vector.VarCharVector]
      val ts = root.getVector("timestamp").asInstanceOf[org.apache.arrow.vector.BigIntVector]
      val n = root.getRowCount
      val rows = new java.util.ArrayList[Row](n)
      var i = 0
      while (i < n) {
        val s = if (svc.isNull(i)) null else svc.getObject(i).toString
        val t = if (ts.isNull(i)) 0L else ts.get(i)
        rows.add(Row(s, t))
        i += 1
      }
      val schema = StructType(Seq(
        StructField("service_name", StringType, nullable = true),
        StructField("timestamp", LongType, nullable = false)
      ))
      spark.createDataFrame(rows, schema)
    } finally {
      root.close()
      alloc.close()
    }
  }
}
