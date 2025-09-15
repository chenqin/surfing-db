package com.pinterest.drsquirrel.jni;

import java.util.Arrays;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.thrift.TBase;

import com.pinterest.drsquirrel.thrift.GenericThriftToArrowConverter;

/** JNI wrapper for native Thrift->Arrow decoding (Binary protocol). */
public final class NativeThriftDecoder {
  static {
    // Try standard lookup first
    boolean ok = false;
    try {
      System.loadLibrary("surfingthriftjni");
      ok = true;
    } catch (UnsatisfiedLinkError e) {
      // Fallback: attempt explicit load from a configured folder
      String[] candidates = new String[] {
          System.getProperty("native.lib.dir"),
          System.getProperty("java.library.path"),
          new java.io.File("..", "build").getAbsolutePath()
      };
      String lib = System.mapLibraryName("surfingthriftjni");
      for (String dir : candidates) {
        if (dir == null || dir.isEmpty()) continue;
        for (String path : dir.split(java.io.File.pathSeparator)) {
          java.io.File f = new java.io.File(path, lib);
          if (f.exists()) {
            System.load(f.getAbsolutePath());
            ok = true;
            break;
          }
        }
        if (ok) break;
      }
      if (!ok) throw e;
    }
  }

  private NativeThriftDecoder() {}

  private static native void decode(byte[][] payloads, String thriftPath, String structName,
                                    long schemaOutAddr, long arrayOutAddr);
  private static native void decodeFromDirect(java.nio.ByteBuffer[] payloads, String thriftPath, String structName,
                                              long schemaOutAddr, long arrayOutAddr);

  /**
   * Convert Thrift payloads to Arrow batch using native C++.
   */
  public static VectorSchemaRoot convert(BufferAllocator allocator,
                                         byte[][] payloads,
                                         String thriftPath,
                                         String structName) {
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
         ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      decode(payloads, thriftPath, structName, outSchema.memoryAddress(), outArray.memoryAddress());
      return Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
    }
  }

  /**
   * Convert Thrift payloads provided as direct ByteBuffers to Arrow batch using native C++.
   * Each ByteBuffer should be a direct buffer whose slice corresponds to the payload bytes.
   * Non-direct buffers will be copied into direct buffers before decoding.
   */
  public static VectorSchemaRoot convert(BufferAllocator allocator,
                                         java.nio.ByteBuffer[] payloads,
                                         String thriftPath,
                                         String structName) {
    // Fallback to byte[] path for maximum compatibility (supports deep nesting today)
    byte[][] arr = new byte[payloads.length][];
    for (int i = 0; i < payloads.length; i++) {
      java.nio.ByteBuffer bb = payloads[i];
      if (bb == null) { arr[i] = new byte[0]; continue; }
      java.nio.ByteBuffer dup = bb.duplicate(); dup.position(0);
      byte[] b = new byte[dup.remaining()];
      dup.get(b);
      arr[i] = b;
    }
    return convert(allocator, arr, thriftPath, structName);
  }

  /**
   * New API: Convert using a generated Thrift class instead of a .thrift path.
   * Falls back to the Java converter (GenericThriftToArrowConverter).
   */
  public static VectorSchemaRoot convert(BufferAllocator allocator,
                                         byte[][] payloads,
                                         Class<? extends TBase<?, ?>> thriftClass) {
    GenericThriftToArrowConverter conv = new GenericThriftToArrowConverter();
    // Convert list of payloads at once
    java.util.List<byte[]> list = Arrays.asList(payloads);
    return conv.convert(list, thriftClass, allocator);
  }
}
