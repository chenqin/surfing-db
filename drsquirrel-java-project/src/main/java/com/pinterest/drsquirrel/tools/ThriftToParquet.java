package com.pinterest.drsquirrel.tools;

import com.pinterest.drsquirrel.jni.NativeThriftDecoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.util.HadoopOutputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;

import java.util.*;

public final class ThriftToParquet {
  private static void usage() {
    System.out.println("Usage: ThriftToParquet [--out path] [--format parquet|csv] [--decoder java|jni] [--thrift-resource path] [--thrift-path path --struct-name Name]");
  }

  public static void main(String[] args) throws Exception {
    String out = "artifacts/thrift_batch.parquet";
    String format = "parquet"; // or csv
    String decoder = "java"; // or jni
    String thriftResource = null;
    String thriftPath = null;
    String structName = null;
    for (int i = 0; i < args.length; i++) {
      switch (args[i]) {
        case "--out": out = args[++i]; break;
        case "--format": format = args[++i]; break;
        case "--decoder": decoder = args[++i]; break;
        case "--thrift-resource": thriftResource = args[++i]; break;
        case "--thrift-path": thriftPath = args[++i]; break;
        case "--struct-name": structName = args[++i]; break;
        case "-h": case "--help": usage(); return;
        default: break;
      }
    }

    // Build sample payloads
    List<byte[]> payloads = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      TMemoryBuffer buf = new TMemoryBuffer(256);
      TBinaryProtocol p = new TBinaryProtocol(buf);
      p.writeStructBegin(new org.apache.thrift.protocol.TStruct("MabsMetrics"));
      p.writeFieldBegin(new org.apache.thrift.protocol.TField("timestamp", org.apache.thrift.protocol.TType.I64, (short)1));
      p.writeI64(1_000_000L + i);
      p.writeFieldEnd();
      p.writeFieldBegin(new org.apache.thrift.protocol.TField("service_name", org.apache.thrift.protocol.TType.STRING, (short)7));
      p.writeString("svc" + (i % 17));
      p.writeFieldEnd();
      p.writeFieldStop();
      p.writeStructEnd();
      payloads.add(Arrays.copyOf(buf.getArray(), buf.length()));
    }

    BufferAllocator alloc = new RootAllocator();
    VectorSchemaRoot root;
    if ("jni".equalsIgnoreCase(decoder)) {
      byte[][] arr = payloads.toArray(new byte[0][]);
      try {
        if (thriftPath != null && structName != null) {
          root = NativeThriftDecoder.convert(alloc, arr, thriftPath, structName);
        } else {
          // fallback to Java if JNI path not fully specified
          org.apache.thrift.ext.GenericThriftToArrowConverter conv = new org.apache.thrift.ext.GenericThriftToArrowConverter();
          root = conv.convert(payloads, com.pinterest.mabs_metrics.thrift.MabsMetrics.class, alloc);
        }
      } catch (Throwable t) {
        org.apache.thrift.ext.GenericThriftToArrowConverter conv = new org.apache.thrift.ext.GenericThriftToArrowConverter();
        root = conv.convert(payloads, com.pinterest.mabs_metrics.thrift.MabsMetrics.class, alloc);
      }
    } else {
      org.apache.thrift.ext.GenericThriftToArrowConverter conv = new org.apache.thrift.ext.GenericThriftToArrowConverter();
      root = conv.convert(payloads, com.pinterest.mabs_metrics.thrift.MabsMetrics.class, alloc);
    }

    try {
      if ("csv".equalsIgnoreCase(format)) {
        String outCsv = out.endsWith(".csv") ? out : out + ".csv";
        try (java.io.FileWriter fw = new java.io.FileWriter(outCsv)) {
          fw.write("service_name,timestamp\n");
          org.apache.arrow.vector.VarCharVector svc = (org.apache.arrow.vector.VarCharVector) root.getVector("service_name");
          org.apache.arrow.vector.BigIntVector ts = (org.apache.arrow.vector.BigIntVector) root.getVector("timestamp");
          int n = root.getRowCount();
          for (int i = 0; i < n; i++) {
            String s = svc.isNull(i) ? "" : svc.getObject(i).toString();
            long t = ts.isNull(i) ? 0 : ts.get(i);
            fw.write(s + "," + t + "\n");
          }
          System.out.println("Wrote CSV: " + outCsv + " rows=" + n);
        }
      } else {
        // Write Parquet using simple Avro fallback for the two projected columns
        String outPath = out.endsWith(".parquet") ? out : out + ".parquet";
        org.apache.arrow.vector.VarCharVector svc = (org.apache.arrow.vector.VarCharVector) root.getVector("service_name");
        org.apache.arrow.vector.BigIntVector ts = (org.apache.arrow.vector.BigIntVector) root.getVector("timestamp");
        java.util.ArrayList<org.apache.avro.generic.GenericRecord> rows = new java.util.ArrayList<>(root.getRowCount());
        String schemaStr = "{\n" +
            " \"type\": \"record\",\n" +
            " \"name\": \"Row\",\n" +
            " \"fields\": [\n" +
            "   {\"name\":\"service_name\",\"type\":[\"null\",\"string\"],\"default\":null},\n" +
            "   {\"name\":\"timestamp\",\"type\":\"long\"}\n" +
            " ]\n" +
            "}";
        org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(schemaStr);
        for (int i = 0; i < root.getRowCount(); i++) {
          org.apache.avro.generic.GenericRecord rec = new org.apache.avro.generic.GenericData.Record(avroSchema);
          String s = svc.isNull(i) ? null : svc.getObject(i).toString();
          long t = ts.isNull(i) ? 0 : ts.get(i);
          rec.put("service_name", s);
          rec.put("timestamp", t);
          rows.add(rec);
        }
        org.apache.parquet.hadoop.metadata.CompressionCodecName codec = org.apache.parquet.hadoop.metadata.CompressionCodecName.SNAPPY;
        try (org.apache.parquet.hadoop.ParquetWriter<org.apache.avro.generic.GenericRecord> writer =
                 org.apache.parquet.avro.AvroParquetWriter.<org.apache.avro.generic.GenericRecord>builder(new Path(outPath))
                     .withSchema(avroSchema)
                     .withCompressionCodec(codec)
                     .withConf(new Configuration())
                     .build()) {
          for (org.apache.avro.generic.GenericRecord r : rows) {
            writer.write(r);
          }
        }
        System.out.println("Wrote Parquet: " + outPath + " rows=" + root.getRowCount());
      }
    } finally {
      root.close();
      alloc.close();
    }
  }
}
