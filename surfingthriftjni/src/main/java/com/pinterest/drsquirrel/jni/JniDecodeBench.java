package com.pinterest.drsquirrel.jni;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.thrift.TBase;

import com.pinterest.deep.bench.DeepEvent;
import com.pinterest.mabs_metrics.thrift.MabsMetrics;
import org.apache.thrift.ext.FastThriftBinaryDecoder;

public final class JniDecodeBench {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: JniDecodeBench <payloads.bin> <thriftPath> <structName> [iters] [mode] \n" +
          "  mode: array (default) | bb (mmap zero-copy) | bbp (pooled direct buffer)");
      System.exit(2);
    }
    String payloadPath = args[0];
    String thriftPath = args[1];
    String structName = args[2];
    int iters = args.length >= 4 ? Integer.parseInt(args[3]) : 5;
    String mode = args.length >= 5 ? args[4] : "array"; // "array" | "bb" | "bbp"

    // Load payloads
    // If mode=="bb", memory-map the file and create direct sliced ByteBuffers (zero-copy).
    // Otherwise, load into byte[] list for array[] JNI path.
    List<byte[]> payloads = null;
    java.nio.ByteBuffer[] bbPayloads = null;
    long totalBytesPayload = 0L;
    int rowsCount = 0;
    if ("bb".equalsIgnoreCase(mode) || "bbp".equalsIgnoreCase(mode)) {
      java.nio.file.Path p = java.nio.file.Paths.get(payloadPath);
      try (java.nio.channels.FileChannel ch = java.nio.channels.FileChannel.open(p, java.nio.file.StandardOpenOption.READ)) {
        long size = ch.size();
        java.nio.MappedByteBuffer mmap = ch.map(java.nio.channels.FileChannel.MapMode.READ_ONLY, 0, size);
        if ("bb".equalsIgnoreCase(mode)) {
          // Iterate: [uint32 LE length][bytes] -> zero-copy slices
          java.util.ArrayList<java.nio.ByteBuffer> list = new java.util.ArrayList<>();
          while (mmap.remaining() >= 4) {
            int b0 = mmap.get() & 0xFF;
            int b1 = mmap.get() & 0xFF;
            int b2 = mmap.get() & 0xFF;
            int b3 = mmap.get() & 0xFF;
            int len = (b0) | (b1 << 8) | (b2 << 16) | (b3 << 24);
            if (len < 0 || mmap.remaining() < len) {
              throw new IllegalStateException("Corrupt payload file: invalid length or truncated");
            }
            int start = mmap.position();
            java.nio.ByteBuffer slice = mmap.slice(); // position=0, remaining=mmap.remaining()
            slice.limit(len);
            list.add(slice);
            mmap.position(start + len);
            totalBytesPayload += len;
          }
          bbPayloads = list.toArray(new java.nio.ByteBuffer[0]);
        } else {
          // Pooled direct buffer mode: copy payload bytes into one large direct buffer, then slice
          // First pass: count rows and total bytes
          java.nio.ByteBuffer scan = mmap.duplicate(); scan.position(0);
          long total = 0; int rows = 0;
          while (scan.remaining() >= 4) {
            int b0 = scan.get() & 0xFF, b1 = scan.get() & 0xFF, b2 = scan.get() & 0xFF, b3 = scan.get() & 0xFF;
            int len = (b0) | (b1 << 8) | (b2 << 16) | (b3 << 24);
            if (scan.remaining() < len) break; // stop at truncation
            scan.position(scan.position() + len);
            total += len; rows++;
          }
          java.nio.ByteBuffer pool = java.nio.ByteBuffer.allocateDirect((int) total);
          bbPayloads = new java.nio.ByteBuffer[rows];
          // Second pass: copy bytes and record slices
          mmap.position(0);
          int offset = 0; int i = 0;
          while (mmap.remaining() >= 4 && i < rows) {
            int b0 = mmap.get() & 0xFF, b1 = mmap.get() & 0xFF, b2 = mmap.get() & 0xFF, b3 = mmap.get() & 0xFF;
            int len = (b0) | (b1 << 8) | (b2 << 16) | (b3 << 24);
            if (mmap.remaining() < len) break;
            // Copy chunk into pool
            java.nio.ByteBuffer src = mmap.slice(); src.limit(len);
            pool.limit(offset + len).position(offset);
            pool.put(src);
            // Slice view
            java.nio.ByteBuffer dup = pool.duplicate(); dup.position(offset); dup.limit(offset + len);
            bbPayloads[i] = dup.slice();
            offset += len; i++; totalBytesPayload += len;
            mmap.position(mmap.position() + len);
          }
        }
      }
      rowsCount = (bbPayloads == null ? 0 : bbPayloads.length);
      System.out.println("Loaded payloads: " + rowsCount);
    } else {
      payloads = new ArrayList<>();
      try (DataInputStream in = new DataInputStream(new FileInputStream(payloadPath))) {
        while (true) {
          try {
            byte[] lenb = new byte[4];
            in.readFully(lenb);
            int len = java.nio.ByteBuffer.wrap(lenb).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
            byte[] buf = new byte[len];
            in.readFully(buf);
            payloads.add(buf);
          } catch (java.io.EOFException eof) {
            break;
          }
        }
      }
      rowsCount = payloads.size();
      for (byte[] b : payloads) totalBytesPayload += b.length;
      System.out.println("Loaded payloads: " + rowsCount);
    }

    BufferAllocator alloc = new RootAllocator();
    // Warmup JNI
    if ("bb".equalsIgnoreCase(mode) || "bbp".equalsIgnoreCase(mode)) {
      try (VectorSchemaRoot root = NativeThriftDecoder.convert(alloc, bbPayloads, thriftPath, structName)) { }
    } else {
      try (VectorSchemaRoot root = NativeThriftDecoder.convert(alloc, payloads.toArray(new byte[0][]), thriftPath, structName)) { }
    }

    long best = Long.MAX_VALUE, sum = 0;
    for (int i = 0; i < iters; i++) {
      long t0 = System.nanoTime();
      if ("bb".equalsIgnoreCase(mode) || "bbp".equalsIgnoreCase(mode)) {
        try (VectorSchemaRoot root = NativeThriftDecoder.convert(alloc, bbPayloads, thriftPath, structName)) {
          int rows = root.getRowCount();
          if (rows != bbPayloads.length) throw new RuntimeException("row mismatch");
        }
      } else {
        try (VectorSchemaRoot root = NativeThriftDecoder.convert(alloc, payloads.toArray(new byte[0][]), thriftPath, structName)) {
        // force materialization
        int rows = root.getRowCount();
        if (rows != payloads.size()) throw new RuntimeException("row mismatch");
        }
      }
      long t1 = System.nanoTime();
      long dt = t1 - t0;
      sum += dt; best = Math.min(best, dt);
      double secs = dt / 1e9;
      double rps = rowsCount / secs;
      double mbps = (totalBytesPayload / (1024.0 * 1024.0)) / secs;
      System.out.printf("JNI Iter %d: %.2f ms, %.1f rows/s, %.1f MB/s\n", i+1, dt/1e6, rps, mbps);
    }
    double bestSecs = best / 1e9;
    double rps = rowsCount / bestSecs;
    double mbps = (totalBytesPayload / (1024.0 * 1024.0)) / bestSecs;
    System.out.printf("JNI Best: %.2f ms, %.1f rows/s, %.1f MB/s\n", best/1e6, rps, mbps);

    // Java baseline using generated TBase classes (FastThriftBinaryDecoder falls back internally for complex schemas)
    Class<? extends TBase<?, ?>> thriftClass = resolveThriftClass(structName);
    if (thriftClass == null) {
      System.out.println("Skipping Java baseline: no generated class for struct " + structName);
      return;
    }
    // Prefer fast protocol-based Java decoder where supported (falls back to generic for complex types)
    if ("array".equalsIgnoreCase(mode)) {
      // Warmup Java path
      try (VectorSchemaRoot root = FastThriftBinaryDecoder.convert(payloads, thriftClass, alloc)) { }
      long jbest = Long.MAX_VALUE;
      for (int i = 0; i < iters; i++) {
        long t0 = System.nanoTime();
        try (VectorSchemaRoot root = FastThriftBinaryDecoder.convert(payloads, thriftClass, alloc)) {
          int rows = root.getRowCount();
          if (rows != payloads.size()) throw new RuntimeException("row mismatch (java)");
        }
        long t1 = System.nanoTime();
        long dt = t1 - t0;
        jbest = Math.min(jbest, dt);
        double secs = dt / 1e9;
        double rps2 = payloads.size() / secs;
        double mbps2 = (payloads.stream().mapToLong(p -> p.length).sum() / (1024.0 * 1024.0)) / secs;
        System.out.printf("Java Iter %d: %.2f ms, %.1f rows/s, %.1f MB/s\n", i+1, dt/1e6, rps2, mbps2);
      }
      double jbestSecs = jbest / 1e9;
      double jrps = payloads.size() / jbestSecs;
      double jmbps = (payloads.stream().mapToLong(p -> p.length).sum() / (1024.0 * 1024.0)) / jbestSecs;
      System.out.printf("Java Best: %.2f ms, %.1f rows/s, %.1f MB/s\n", jbest/1e6, jrps, jmbps);
    } else {
      // Large payloads via ByteBuffer: copy to byte[] once for Java baseline
      List<byte[]> jpayloads = new ArrayList<>(rowsCount);
      long total = 0L;
      for (java.nio.ByteBuffer bb : bbPayloads) {
        if (bb == null) { jpayloads.add(new byte[0]); continue; }
        java.nio.ByteBuffer dup = bb.duplicate(); dup.position(0);
        byte[] arr = new byte[dup.remaining()];
        dup.get(arr);
        total += arr.length;
        jpayloads.add(arr);
      }
      // Warmup
      try (VectorSchemaRoot root = FastThriftBinaryDecoder.convert(jpayloads, thriftClass, alloc)) { }
      long jbest = Long.MAX_VALUE;
      for (int i = 0; i < iters; i++) {
        long t0 = System.nanoTime();
        try (VectorSchemaRoot root = FastThriftBinaryDecoder.convert(jpayloads, thriftClass, alloc)) {
          int rows = root.getRowCount();
          if (rows != jpayloads.size()) throw new RuntimeException("row mismatch (java)");
        }
        long t1 = System.nanoTime();
        long dt = t1 - t0;
        jbest = Math.min(jbest, dt);
        double secs = dt / 1e9;
        double rps2 = jpayloads.size() / secs;
        double mbps2 = (total / (1024.0 * 1024.0)) / secs;
        System.out.printf("Java Iter %d: %.2f ms, %.1f rows/s, %.1f MB/s\n", i+1, dt/1e6, rps2, mbps2);
      }
      double jbestSecs = jbest / 1e9;
      double jrps = jpayloads.size() / jbestSecs;
      double jmbps = (total / (1024.0 * 1024.0)) / jbestSecs;
      System.out.printf("Java Best: %.2f ms, %.1f rows/s, %.1f MB/s\n", jbest/1e6, jrps, jmbps);
    }
    alloc.close();
  }

  private static Class<? extends TBase<?, ?>> resolveThriftClass(String structName) {
    switch (structName) {
      case "MabsMetrics":
        return MabsMetrics.class;
      case "DeepEvent":
        return DeepEvent.class;
      default:
        return null;
    }
  }
}
