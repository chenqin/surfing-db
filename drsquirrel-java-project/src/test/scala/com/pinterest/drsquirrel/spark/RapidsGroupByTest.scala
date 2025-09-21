package com.pinterest.drsquirrel.spark

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

import junit.framework.{Assert, TestCase}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.{BigIntVector, VarCharVector}
import org.apache.thrift.protocol.{TBinaryProtocol, TField, TStruct, TType}
import org.apache.thrift.transport.TMemoryBuffer

class RapidsGroupByTest extends TestCase {

  private def writeBase64Payload(payload: Array[Byte]): Path = {
    val tmp = Files.createTempFile("rapids-groupby", ".b64")
    tmp.toFile.deleteOnExit()
    val value = java.util.Base64.getEncoder.encodeToString(payload) + "\n"
    Files.write(tmp, value.getBytes(StandardCharsets.UTF_8))
    tmp
  }

  private def buildMabsMetricsPayload(service: String, timestamp: Long): Array[Byte] = {
    val buf = new TMemoryBuffer(256)
    val proto = new TBinaryProtocol(buf)
    proto.writeStructBegin(new TStruct("MabsMetrics"))
    proto.writeFieldBegin(new TField("timestamp", TType.I64, 1))
    proto.writeI64(timestamp)
    proto.writeFieldEnd()
    proto.writeFieldBegin(new TField("service_name", TType.STRING, 7))
    proto.writeString(service)
    proto.writeFieldEnd()
    proto.writeFieldStop()
    proto.writeStructEnd()
    java.util.Arrays.copyOf(buf.getArray, buf.length())
  }

  def testJavaDecoderConvertsThriftPayloadsToArrowBatch(): Unit = {
    val payload = buildMabsMetricsPayload("metric", 12345L)
    val path = writeBase64Payload(payload)
    val cfg = RapidsGroupBy.Config(
      inputPath = path.toString,
      inputMode = "thrift",
      inFormat = "lines-base64",
      decoder = "java"
    )

    val alloc = new RootAllocator()
    try {
      val root = RapidsGroupBy.convertThriftPayloads(cfg, alloc)
      try {
        Assert.assertEquals(1, root.getRowCount)
        val svc = root.getVector("service_name").asInstanceOf[VarCharVector]
        val ts = root.getVector("timestamp").asInstanceOf[BigIntVector]
        Assert.assertEquals("metric", svc.getObject(0).toString)
        Assert.assertEquals(12345L, ts.get(0))
      } finally {
        root.close()
      }
    } finally {
      alloc.close()
    }
  }

  def testJniDecoderFallsBackToJavaWhenNativeUnavailable(): Unit = {
    val payload = buildMabsMetricsPayload("gpu-service", 42L)
    val path = writeBase64Payload(payload)
    val cfg = RapidsGroupBy.Config(
      inputPath = path.toString,
      inputMode = "thrift",
      inFormat = "lines-base64",
      decoder = "jni",
      thriftPath = new java.io.File("src/test/resources/schemas/mabs.thrift").getAbsolutePath,
      structName = "MabsMetrics"
    )

    val alloc = new RootAllocator()
    try {
      val root = RapidsGroupBy.convertThriftPayloads(cfg, alloc)
      try {
        Assert.assertEquals(1, root.getRowCount)
        val svc = root.getVector("service_name").asInstanceOf[VarCharVector]
        val ts = root.getVector("timestamp").asInstanceOf[BigIntVector]
        Assert.assertEquals("gpu-service", svc.getObject(0).toString)
        Assert.assertEquals(42L, ts.get(0))
      } finally {
        root.close()
      }
    } finally {
      alloc.close()
    }
  }
}
