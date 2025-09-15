package com.pinterest.drsquirrel.jni;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

public final class JniDecodeBench {
  public static void main(String[] args) throws Exception {
    if (args.length < 3) {
      System.err.println("Usage: JniDecodeBench <payloads.bin> <thriftPath> <structName> [iters]");
      System.exit(2);
    }
    String payloadPath = args[0];
    String thriftPath = args[1];
    String structName = args[2];
    int iters = args.length >= 4 ? Integer.parseInt(args[3]) : 5;

    // Load payloads (format: [uint32 length][bytes] repeated)
    List<byte[]> payloads = new ArrayList<>();
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
    System.out.println("Loaded payloads: " + payloads.size());

    BufferAllocator alloc = new RootAllocator();
    // Warmup
    try (VectorSchemaRoot root = NativeThriftDecoder.convert(alloc, payloads.toArray(new byte[0][]), thriftPath, structName)) {
      // noop
    }

    long best = Long.MAX_VALUE, sum = 0;
    long totalBytes = payloads.stream().mapToLong(p -> p.length).sum();
    for (int i = 0; i < iters; i++) {
      long t0 = System.nanoTime();
      try (VectorSchemaRoot root = NativeThriftDecoder.convert(alloc, payloads.toArray(new byte[0][]), thriftPath, structName)) {
        // force materialization
        int rows = root.getRowCount();
        if (rows != payloads.size()) throw new RuntimeException("row mismatch");
      }
      long t1 = System.nanoTime();
      long dt = t1 - t0;
      sum += dt; best = Math.min(best, dt);
      double secs = dt / 1e9;
      double rps = payloads.size() / secs;
      double mbps = (totalBytes / (1024.0 * 1024.0)) / secs;
      System.out.printf("Iter %d: %.2f ms, %.1f rows/s, %.1f MB/s\n", i+1, dt/1e6, rps, mbps);
    }
    double bestSecs = best / 1e9;
    double rps = payloads.size() / bestSecs;
    double mbps = (totalBytes / (1024.0 * 1024.0)) / bestSecs;
    System.out.printf("Best: %.2f ms, %.1f rows/s, %.1f MB/s\n", best/1e6, rps, mbps);
    alloc.close();
  }
}
