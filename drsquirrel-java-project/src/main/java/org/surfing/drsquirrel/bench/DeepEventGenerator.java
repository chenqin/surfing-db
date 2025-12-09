package org.surfing.drsquirrel.bench;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Random;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;

/**
 * Writes DeepEvent payloads (Binary protocol) matching src/bench/deep_event.thrift.
 * Output format: [uint32 length][bytes] repeated.
 */
public final class DeepEventGenerator {
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: DeepEventGenerator <out.bin> <rows> [seed]");
      System.exit(2);
    }
    String out = args[0];
    int rows = Integer.parseInt(args[1]);
    long seed = args.length >= 3 ? Long.parseLong(args[2]) : 12345L;
    Random rnd = new Random(seed);
    try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(out))) {
      for (int i = 0; i < rows; i++) {
        TMemoryBuffer buf = new TMemoryBuffer(1024);
        TBinaryProtocol proto = new TBinaryProtocol(buf);
        writeDeepEvent(proto, i, rnd);
        byte[] bytes = buf.getArray();
        int len = buf.length();
        // write length as little-endian uint32 to match readers
        dos.writeByte(len & 0xFF);
        dos.writeByte((len >>> 8) & 0xFF);
        dos.writeByte((len >>> 16) & 0xFF);
        dos.writeByte((len >>> 24) & 0xFF);
        dos.write(bytes, 0, len);
      }
    }
    System.err.printf("Wrote %d DeepEvent payloads to %s%n", rows, out);
  }

  private static void writeDeepEvent(TBinaryProtocol p, int idx, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("DeepEvent"));
    // 1: event_id
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("event_id", TType.I64, (short)1));
    p.writeI64(1_000_000L + idx);
    p.writeFieldEnd();
    // 2: source
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("source", TType.STRING, (short)2));
    p.writeString("src-" + (idx % 10));
    p.writeFieldEnd();
    // 3: metrics list<list<i32>>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("metrics", TType.LIST, (short)3));
    int outer = 1 + rnd.nextInt(3);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.LIST, outer));
    for (int i = 0; i < outer; i++) {
      int inner = 1 + rnd.nextInt(5);
      p.writeListBegin(new org.apache.thrift.protocol.TList(TType.I32, inner));
      for (int j = 0; j < inner; j++) p.writeI32(rnd.nextInt(100));
      p.writeListEnd();
    }
    p.writeListEnd();
    p.writeFieldEnd();
    // 4: label_ids set<i64>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("label_ids", TType.SET, (short)4));
    int sc = 1 + rnd.nextInt(4);
    p.writeSetBegin(new org.apache.thrift.protocol.TSet(TType.I64, sc));
    for (int i = 0; i < sc; i++) p.writeI64(rnd.nextInt(1000));
    p.writeSetEnd();
    p.writeFieldEnd();
    // 5: counts_by_key map<string, list<i64>>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("counts_by_key", TType.MAP, (short)5));
    int mc = 1 + rnd.nextInt(3);
    p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.LIST, mc));
    for (int i = 0; i < mc; i++) {
      p.writeString("k" + i);
      int lc = 1 + rnd.nextInt(3);
      p.writeListBegin(new org.apache.thrift.protocol.TList(TType.I64, lc));
      for (int j = 0; j < lc; j++) p.writeI64(rnd.nextInt(100000));
      p.writeListEnd();
    }
    p.writeMapEnd();
    p.writeFieldEnd();
    // 6: meta struct Meta
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("meta", TType.STRUCT, (short)6));
    writeMeta(p, rnd);
    p.writeFieldEnd();
    // 7: readings list<Reading>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("readings", TType.LIST, (short)7));
    int rc = 1 + rnd.nextInt(4);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, rc));
    for (int i = 0; i < rc; i++) writeReading(p, idx, i, rnd);
    p.writeListEnd();
    p.writeFieldEnd();
    // 8: bundles map<string, Bundle>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("bundles", TType.MAP, (short)8));
    int bc = 1 + rnd.nextInt(3);
    p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.STRUCT, bc));
    for (int i = 0; i < bc; i++) {
      p.writeString("b" + i);
      writeBundle(p, idx, rnd);
    }
    p.writeMapEnd();
    p.writeFieldEnd();
    // 9: attr_maps list<map<string, list<Attr>>>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("attr_maps", TType.LIST, (short)9));
    int am = 1 + rnd.nextInt(3);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.MAP, am));
    for (int i = 0; i < am; i++) {
      int msz = 1 + rnd.nextInt(2);
      p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.LIST, msz));
      for (int j = 0; j < msz; j++) {
        p.writeString("a" + j);
        int lsz = 1 + rnd.nextInt(3);
        p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, lsz));
        for (int k = 0; k < lsz; k++) writeAttr(p, rnd);
        p.writeListEnd();
      }
      p.writeMapEnd();
    }
    p.writeListEnd();
    p.writeFieldEnd();
    // 10: geo
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("geo", TType.STRUCT, (short)10));
    writeGeo(p, rnd);
    p.writeFieldEnd();
    // 11: deep_universe (five-level nested map/list/struct)
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("deep_universe", TType.STRUCT, (short)11));
    writeDeepUniverse(p, rnd);
    p.writeFieldEnd();

    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeAttr(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Attr"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("key", TType.STRING, (short)1)); p.writeString(randStr(rnd, 4)); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("val", TType.STRING, (short)2)); p.writeString(randStr(rnd, 6)); p.writeFieldEnd();
    p.writeFieldStop(); p.writeStructEnd();
  }

  private static void writeReading(TBinaryProtocol p, int base, int i, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Reading"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("ts", TType.I64, (short)1)); p.writeI64(System.currentTimeMillis() + i); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("value", TType.DOUBLE, (short)2)); p.writeDouble(rnd.nextDouble() * 100.0); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("notes", TType.LIST, (short)3));
    int n = 1 + rnd.nextInt(3); p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRING, n));
    for (int j = 0; j < n; j++) p.writeString("n" + j);
    p.writeListEnd(); p.writeFieldEnd();
    p.writeFieldStop(); p.writeStructEnd();
  }

  private static void writeBundle(TBinaryProtocol p, int base, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Bundle"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("items", TType.LIST, (short)1));
    int m = 1 + rnd.nextInt(3); p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, m));
    for (int i = 0; i < m; i++) writeReading(p, base, i, rnd);
    p.writeListEnd(); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("extras", TType.MAP, (short)2));
    int ex = 1 + rnd.nextInt(2); p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.LIST, ex));
    for (int i = 0; i < ex; i++) { p.writeString("x" + i); int nn = 1 + rnd.nextInt(3); p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRING, nn)); for (int j = 0; j < nn; j++) p.writeString("e" + j); p.writeListEnd(); }
    p.writeMapEnd(); p.writeFieldEnd();
    p.writeFieldStop(); p.writeStructEnd();
  }

  private static void writeMeta(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Meta"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("labels", TType.MAP, (short)1));
    int m = 2; p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.STRING, m));
    p.writeString("env"); p.writeString("prod"); p.writeString("team"); p.writeString("core");
    p.writeMapEnd(); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("kvs", TType.LIST, (short)2));
    int n = 1 + rnd.nextInt(2); p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, n));
    for (int i = 0; i < n; i++) writeAttr(p, rnd);
    p.writeListEnd(); p.writeFieldEnd();
    p.writeFieldStop(); p.writeStructEnd();
  }

  private static void writeGeo(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Geo"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("lat", TType.DOUBLE, (short)1)); p.writeDouble(37.7 + rnd.nextDouble()); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("lon", TType.DOUBLE, (short)2)); p.writeDouble(-122.4 + rnd.nextDouble()); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("region", TType.STRUCT, (short)3));
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Region"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("country", TType.STRING, (short)1)); p.writeString("US"); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("city", TType.STRING, (short)2)); p.writeString("SF"); p.writeFieldEnd();
    p.writeFieldStop(); p.writeStructEnd();
    p.writeFieldEnd();
    p.writeFieldStop(); p.writeStructEnd();
  }

  private static void writeDeepLeaf(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("DeepLeaf"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("leaf_id", TType.STRING, (short)1));
    p.writeString("leaf-" + randStr(rnd, 6));
    p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("samples", TType.LIST, (short)2));
    int count = 2 + rnd.nextInt(4);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.I64, count));
    for (int i = 0; i < count; i++) {
      p.writeI64(Math.abs(rnd.nextLong()) % 1_000_000L);
    }
    p.writeListEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeDeepBranch(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("DeepBranch"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("leaves", TType.MAP, (short)1));
    int entries = 1 + rnd.nextInt(3);
    p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.STRUCT, entries));
    for (int i = 0; i < entries; i++) {
      p.writeString("leaf_key_" + i);
      writeDeepLeaf(p, rnd);
    }
    p.writeMapEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeDeepRoot(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("DeepRoot"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("branches", TType.LIST, (short)1));
    int branches = 1 + rnd.nextInt(3);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, branches));
    for (int i = 0; i < branches; i++) {
      writeDeepBranch(p, rnd);
    }
    p.writeListEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeDeepForest(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("DeepForest"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("roots", TType.MAP, (short)1));
    int roots = 1 + rnd.nextInt(3);
    p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.STRUCT, roots));
    for (int i = 0; i < roots; i++) {
      p.writeString("root_" + i);
      writeDeepRoot(p, rnd);
    }
    p.writeMapEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeDeepUniverse(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("DeepUniverse"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("forests", TType.LIST, (short)1));
    int forests = 1 + rnd.nextInt(3);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, forests));
    for (int i = 0; i < forests; i++) {
      writeDeepForest(p, rnd);
    }
    p.writeListEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static String randStr(Random rnd, int n) {
    String s = "abcdefghijklmnopqrstuvwxyz";
    StringBuilder sb = new StringBuilder(n);
    for (int i = 0; i < n; i++) sb.append(s.charAt(rnd.nextInt(s.length())));
    return sb.toString();
  }
}
