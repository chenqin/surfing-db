package com.pinterest.deep.bench;

import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.util.Random;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TType;
import org.apache.thrift.transport.TMemoryBuffer;

/**
 * Generates DeepEvent payloads in the standard Surfing binary format:
 * repeated [uint32 little-endian length][Thrift binary payload].
 *
 * Small mode mirrors the historical DeepEvent benchmark generator (moderate nesting).
 * Large mode inflates collection sizes to exercise large-payload paths.
 */
public final class DeepEventPayloadGenerator {

  private enum Mode { SMALL, LARGE }

  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      System.err.println("Usage: DeepEventPayloadGenerator <out.bin> <rows> [mode] [seed]\n" +
          "  mode: small (default) | large");
      System.exit(2);
    }
    String out = args[0];
    int rows = Integer.parseInt(args[1]);
    Mode mode = args.length >= 3 ? Mode.valueOf(args[2].toUpperCase()) : Mode.SMALL;
    long seed = args.length >= 4 ? Long.parseLong(args[3]) : 12345L;

    Random rnd = new Random(seed);
    try (DataOutputStream dos = new DataOutputStream(new FileOutputStream(out))) {
      for (int i = 0; i < rows; i++) {
        // Large mode needs extra headroom for nested collections.
        TMemoryBuffer buf = new TMemoryBuffer(mode == Mode.LARGE ? 65536 : 4096);
        TBinaryProtocol proto = new TBinaryProtocol(buf);
        writeDeepEvent(proto, i, rnd, mode);
        byte[] bytes = buf.getArray();
        int len = buf.length();
        // Write little-endian length prefix
        dos.writeByte(len & 0xFF);
        dos.writeByte((len >>> 8) & 0xFF);
        dos.writeByte((len >>> 16) & 0xFF);
        dos.writeByte((len >>> 24) & 0xFF);
        dos.write(bytes, 0, len);
      }
    }
    System.err.printf("Wrote %d DeepEvent payloads (%s) to %s%n", rows, mode.name().toLowerCase(), out);
  }

  private static void writeDeepEvent(TBinaryProtocol p, int idx, Random rnd, Mode mode) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("DeepEvent"));
    // 1: event_id
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("event_id", TType.I64, (short)1));
    p.writeI64(1_000_000L + idx);
    p.writeFieldEnd();
    // 2: source
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("source", TType.STRING, (short)2));
    p.writeString("src-" + (idx % 16));
    p.writeFieldEnd();
    // 3: metrics list<list<i32>>
    int outerMax = mode == Mode.LARGE ? 16 : 3;
    int innerMax = mode == Mode.LARGE ? 32 : 5;
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("metrics", TType.LIST, (short)3));
    int outer = 1 + rnd.nextInt(outerMax);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.LIST, outer));
    for (int i = 0; i < outer; i++) {
      int inner = 1 + rnd.nextInt(innerMax);
      p.writeListBegin(new org.apache.thrift.protocol.TList(TType.I32, inner));
      for (int j = 0; j < inner; j++) p.writeI32(rnd.nextInt(1000));
      p.writeListEnd();
    }
    p.writeListEnd();
    p.writeFieldEnd();
    // 4: label_ids set<i64>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("label_ids", TType.SET, (short)4));
    int setCount = 1 + rnd.nextInt(mode == Mode.LARGE ? 16 : 4);
    p.writeSetBegin(new org.apache.thrift.protocol.TSet(TType.I64, setCount));
    for (int i = 0; i < setCount; i++) p.writeI64(Math.abs(rnd.nextLong()) % 1_000_000L);
    p.writeSetEnd();
    p.writeFieldEnd();
    // 5: counts_by_key map<string, list<i64>>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("counts_by_key", TType.MAP, (short)5));
    int mapSize = 1 + rnd.nextInt(mode == Mode.LARGE ? 16 : 3);
    p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.LIST, mapSize));
    for (int i = 0; i < mapSize; i++) {
      p.writeString("k" + i);
      int listSize = 1 + rnd.nextInt(mode == Mode.LARGE ? 24 : 3);
      p.writeListBegin(new org.apache.thrift.protocol.TList(TType.I64, listSize));
      for (int j = 0; j < listSize; j++) p.writeI64(Math.abs(rnd.nextLong()) % 10_000_000L);
      p.writeListEnd();
    }
    p.writeMapEnd();
    p.writeFieldEnd();
    // 6: meta struct
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("meta", TType.STRUCT, (short)6));
    writeMeta(p, rnd, mode);
    p.writeFieldEnd();
    // 7: readings list<Reading>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("readings", TType.LIST, (short)7));
    int readingCount = 1 + rnd.nextInt(mode == Mode.LARGE ? 24 : 4);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, readingCount));
    for (int i = 0; i < readingCount; i++) writeReading(p, idx, i, rnd, mode);
    p.writeListEnd();
    p.writeFieldEnd();
    // 8: bundles map<string, Bundle>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("bundles", TType.MAP, (short)8));
    int bundleCount = 1 + rnd.nextInt(mode == Mode.LARGE ? 12 : 3);
    p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.STRUCT, bundleCount));
    for (int i = 0; i < bundleCount; i++) {
      p.writeString("b" + i);
      writeBundle(p, idx, rnd, mode);
    }
    p.writeMapEnd();
    p.writeFieldEnd();
    // 9: attr_maps list<map<string, list<Attr>>>
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("attr_maps", TType.LIST, (short)9));
    int attrMaps = 1 + rnd.nextInt(mode == Mode.LARGE ? 10 : 3);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.MAP, attrMaps));
    for (int i = 0; i < attrMaps; i++) {
      int mapEntries = 1 + rnd.nextInt(mode == Mode.LARGE ? 10 : 2);
      p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.LIST, mapEntries));
      for (int j = 0; j < mapEntries; j++) {
        p.writeString("a" + j);
        int attrList = 1 + rnd.nextInt(mode == Mode.LARGE ? 12 : 3);
        p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, attrList));
        for (int k = 0; k < attrList; k++) writeAttr(p, rnd, mode);
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

    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeAttr(TBinaryProtocol p, Random rnd, Mode mode) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Attr"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("key", TType.STRING, (short)1));
    p.writeString(randStr(rnd, mode == Mode.LARGE ? 12 : 4));
    p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("val", TType.STRING, (short)2));
    p.writeString(randStr(rnd, mode == Mode.LARGE ? 20 : 6));
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeReading(TBinaryProtocol p, int base, int i, Random rnd, Mode mode) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Reading"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("ts", TType.I64, (short)1));
    p.writeI64(System.currentTimeMillis() + base + i);
    p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("value", TType.DOUBLE, (short)2));
    p.writeDouble(rnd.nextDouble() * 100.0);
    p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("notes", TType.LIST, (short)3));
    int notes = 1 + rnd.nextInt(mode == Mode.LARGE ? 12 : 3);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRING, notes));
    for (int j = 0; j < notes; j++) p.writeString("n" + j);
    p.writeListEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeBundle(TBinaryProtocol p, int base, Random rnd, Mode mode) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Bundle"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("items", TType.LIST, (short)1));
    int items = 1 + rnd.nextInt(mode == Mode.LARGE ? 10 : 3);
    p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRUCT, items));
    for (int i = 0; i < items; i++) writeReading(p, base, i, rnd, mode);
    p.writeListEnd();
    p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("extras", TType.MAP, (short)2));
    int extras = 1 + rnd.nextInt(mode == Mode.LARGE ? 10 : 2);
    p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.LIST, extras));
    for (int i = 0; i < extras; i++) {
      p.writeString("x" + i);
      int vs = 1 + rnd.nextInt(mode == Mode.LARGE ? 16 : 3);
      p.writeListBegin(new org.apache.thrift.protocol.TList(TType.STRING, vs));
      for (int j = 0; j < vs; j++) p.writeString("e" + j);
      p.writeListEnd();
    }
    p.writeMapEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeMeta(TBinaryProtocol p, Random rnd, Mode mode) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Meta"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("labels", TType.MAP, (short)1));
    int labels = mode == Mode.LARGE ? 12 : 2;
    p.writeMapBegin(new org.apache.thrift.protocol.TMap(TType.STRING, TType.STRING, labels));
    for (int i = 0; i < labels; i++) {
      p.writeString("l" + i);
      p.writeString(randStr(rnd, mode == Mode.LARGE ? 16 : 5));
    }
    p.writeMapEnd();
    p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("kvs", TType.LIST, (short)2));
    int kvs = 1 + rnd.nextInt(mode == Mode.LARGE ? 12 : 3);
    p.writeListBegin(new TList(TType.STRUCT, kvs));
    for (int i = 0; i < kvs; i++) writeAttr(p, rnd, mode);
    p.writeListEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static void writeGeo(TBinaryProtocol p, Random rnd) throws Exception {
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Geo"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("lat", TType.DOUBLE, (short)1));
    p.writeDouble(37.7 + rnd.nextDouble());
    p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("lon", TType.DOUBLE, (short)2));
    p.writeDouble(-122.4 + rnd.nextDouble());
    p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("region", TType.STRUCT, (short)3));
    p.writeStructBegin(new org.apache.thrift.protocol.TStruct("Region"));
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("country", TType.STRING, (short)1)); p.writeString("US"); p.writeFieldEnd();
    p.writeFieldBegin(new org.apache.thrift.protocol.TField("city", TType.STRING, (short)2)); p.writeString("SF"); p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
    p.writeFieldEnd();
    p.writeFieldStop();
    p.writeStructEnd();
  }

  private static String randStr(Random rnd, int len) {
    final String alphabet = "abcdefghijklmnopqrstuvwxyz";
    StringBuilder sb = new StringBuilder(len);
    for (int i = 0; i < len; i++) sb.append(alphabet.charAt(rnd.nextInt(alphabet.length())));
    return sb.toString();
  }
}
