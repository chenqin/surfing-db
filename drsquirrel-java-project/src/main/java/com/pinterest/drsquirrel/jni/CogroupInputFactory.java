package com.pinterest.drsquirrel.jni;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Helpers for constructing cogroup inputs from Thrift payload files via surfingthriftjni.
 */
public final class CogroupInputFactory {

  private CogroupInputFactory() {}

  /** Container for decoded Arrow batches that can be fed to {@link NativeProcessors#cogroup}. */
  public static final class ThriftInputs implements AutoCloseable {
    public final VectorSchemaRoot left;
    public final VectorSchemaRoot right;
    public final String keyField;
    public final long rowsPerRank;
    public final double leftDecodeSeconds;
    public final double rightDecodeSeconds;
    public final long leftBytes;
    public final long rightBytes;

    ThriftInputs(VectorSchemaRoot left,
                 VectorSchemaRoot right,
                 String keyField,
                 long rowsPerRank,
                 double leftDecodeSeconds,
                 double rightDecodeSeconds,
                 long leftBytes,
                 long rightBytes) {
      this.left = Objects.requireNonNull(left, "left");
      this.right = Objects.requireNonNull(right, "right");
      this.keyField = Objects.requireNonNull(keyField, "keyField");
      this.rowsPerRank = rowsPerRank;
      this.leftDecodeSeconds = leftDecodeSeconds;
      this.rightDecodeSeconds = rightDecodeSeconds;
      this.leftBytes = leftBytes;
      this.rightBytes = rightBytes;
    }

    @Override
    public void close() {
      left.close();
      if (right != left) {
        right.close();
      }
    }
  }

  public static ThriftInputs fromThrift(BufferAllocator allocator,
                                        String thriftPath,
                                        String structName,
                                        String leftPayloadPath,
                                        String rightPayloadPath,
                                        String keyField,
                                        int rank,
                                        int world) throws IOException {
    if (allocator == null) {
      throw new IllegalArgumentException("allocator must not be null");
    }
    Objects.requireNonNull(thriftPath, "thriftPath");
    Objects.requireNonNull(structName, "structName");
    Objects.requireNonNull(leftPayloadPath, "leftPayloadPath");

    Path thrift = Paths.get(thriftPath);
    Path leftPath = Paths.get(leftPayloadPath);
    Path rightPath = rightPayloadPath != null ? Paths.get(rightPayloadPath) : null;
    if (!Files.exists(thrift)) {
      throw new IOException("Thrift schema not found: " + thrift.toAbsolutePath());
    }
    if (!Files.exists(leftPath)) {
      throw new IOException("Left payload file not found: " + leftPath.toAbsolutePath());
    }
    if (rightPath != null && !Files.exists(rightPath)) {
      throw new IOException("Right payload file not found: " + rightPath.toAbsolutePath());
    }

    byte[][] leftAll = readPayloadFile(leftPath);
    if (leftAll.length == 0) {
      throw new IOException("Left payload file is empty: " + leftPath.toAbsolutePath());
    }
    byte[][] leftPartition = partitionPayloads(leftAll, rank, world);
    if (leftPartition.length == 0) {
      throw new IOException("Left payload partition is empty for rank " + rank + " in world " + world);
    }
    long leftBytes = sumBytes(leftPartition);

    byte[][] rightPartition;
    if (rightPath != null) {
      byte[][] rightAll = readPayloadFile(rightPath);
      if (rightAll.length == 0) {
        throw new IOException("Right payload file is empty: " + rightPath.toAbsolutePath());
      }
      rightPartition = partitionPayloads(rightAll, rank, world);
      if (rightPartition.length == 0) {
        throw new IOException("Right payload partition is empty for rank " + rank + " in world " + world);
      }
      if (rightPartition == leftPartition) {
        // Shouldn't happen, but guard against aliasing.
        rightPartition = rightPartition.clone();
      }
    } else {
      // If no explicit RHS payloads provided, reuse left payloads (but decode into a fresh batch).
      rightPartition = leftPartition;
    }
    long rightBytes = sumBytes(rightPartition);

    VectorSchemaRoot leftRoot = null;
    VectorSchemaRoot rightRoot = null;
    try {
      long t0 = System.nanoTime();
      leftRoot = NativeThriftDecoder.convert(allocator, leftPartition, thrift.toString(), structName);
      double leftSeconds = (System.nanoTime() - t0) / 1e9;

      long t1 = System.nanoTime();
      rightRoot = NativeThriftDecoder.convert(allocator, rightPartition, thrift.toString(), structName);
      double rightSeconds = (System.nanoTime() - t1) / 1e9;

      long rowsPerRank = leftRoot.getRowCount();
      if (rowsPerRank == 0) {
        throw new IOException("Decoded left batch is empty for rank " + rank);
      }
      if (rightRoot.getRowCount() == 0) {
        throw new IOException("Decoded right batch is empty for rank " + rank);
      }
      if (keyField == null || keyField.isEmpty()) {
        keyField = leftRoot.getSchema().getFields().isEmpty()
            ? ""
            : leftRoot.getSchema().getFields().get(0).getName();
      }
      if (keyField == null || keyField.isEmpty()) {
        throw new IOException("Unable to resolve key field for decoded schema");
      }
      if (leftRoot.getSchema().findField(keyField) == null) {
        throw new IOException("Key field '" + keyField + "' not present in decoded left schema");
      }
      if (rightRoot.getSchema().findField(keyField) == null) {
        throw new IOException("Key field '" + keyField + "' not present in decoded right schema");
      }
      return new ThriftInputs(leftRoot, rightRoot, keyField, rowsPerRank, leftSeconds, rightSeconds, leftBytes, rightBytes);
    } catch (Throwable t) {
      if (leftRoot != null) {
        leftRoot.close();
      }
      if (rightRoot != null) {
        rightRoot.close();
      }
      if (t instanceof IOException) {
        throw (IOException) t;
      }
      throw new IOException("Failed to decode Thrift payloads", t);
    }
  }

  private static byte[][] readPayloadFile(Path path) throws IOException {
    List<byte[]> payloads = new ArrayList<>();
    try (DataInputStream in = new DataInputStream(Files.newInputStream(path))) {
      byte[] lenBuf = new byte[4];
      while (true) {
        try {
          in.readFully(lenBuf);
        } catch (EOFException eof) {
          break;
        }
        int len = ByteBuffer.wrap(lenBuf).order(ByteOrder.LITTLE_ENDIAN).getInt();
        if (len < 0) {
          throw new IOException("Negative payload length in " + path.toAbsolutePath());
        }
        byte[] payload = new byte[len];
        in.readFully(payload);
        payloads.add(payload);
      }
    }
    return payloads.toArray(new byte[0][]);
  }

  private static byte[][] partitionPayloads(byte[][] payloads, int rank, int world) {
    List<byte[]> out = new ArrayList<>();
    for (int i = rank; i < payloads.length; i += Math.max(world, 1)) {
      out.add(payloads[i]);
    }
    return out.toArray(new byte[0][]);
  }

  private static long sumBytes(byte[][] payloads) {
    long sum = 0L;
    for (byte[] payload : payloads) {
      if (payload != null) {
        sum += payload.length;
      }
    }
    return sum;
  }
}
