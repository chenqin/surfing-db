package org.surfing.drsquirrel.jni;

import java.util.Arrays;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.thrift.TBase;

import org.apache.thrift.ext.GenericThriftToArrowConverter;

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
      // Final fallback: attempt to load from a natives jar under META-INF/lib/<os>-<arch>/
      if (!ok) {
        try {
          String os = System.getProperty("os.name").toLowerCase(java.util.Locale.ROOT);
          String arch = System.getProperty("os.arch").toLowerCase(java.util.Locale.ROOT);
          String osNorm;
          if (os.contains("mac") || os.contains("darwin")) osNorm = "osx";
          else if (os.contains("linux")) osNorm = "linux";
          else if (os.contains("win")) osNorm = "windows";
          else osNorm = os;
          String archNorm = arch;
          if ("amd64".equals(archNorm)) archNorm = "x86_64";
          if ("x86-64".equals(archNorm)) archNorm = "x86_64";
          if ("aarch64".equals(archNorm)) archNorm = "aarch_64";
          String mapped = System.mapLibraryName("surfingthriftjni");
          String resPath = String.format("/META-INF/lib/%s-%s/%s", osNorm, archNorm, mapped);
          try (java.io.InputStream in = NativeThriftDecoder.class.getResourceAsStream(resPath)) {
            if (in != null) {
              java.nio.file.Path tmp = java.nio.file.Files.createTempFile("surfingthriftjni-", mapped);
              tmp.toFile().deleteOnExit();
              java.nio.file.Files.copy(in, tmp, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
              System.load(tmp.toAbsolutePath().toString());
              ok = true;
            }
          }
        } catch (Throwable t) {
          // ignore, fall through to throw original error
        }
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
    // For large batches, copy payloads into direct ByteBuffers and use the native
    // direct path which supports multi-threaded decode internally.
    final boolean forceDirect = Boolean.parseBoolean(System.getProperty("surfing.decode.toDirect", "true"));
    final int threshold = Integer.getInteger("surfing.decode.toDirectThreshold", 1024);
    if (forceDirect && payloads != null && payloads.length >= threshold) {
      java.nio.ByteBuffer[] direct = new java.nio.ByteBuffer[payloads.length];
      for (int i = 0; i < payloads.length; i++) {
        byte[] p = payloads[i];
        if (p == null) { direct[i] = java.nio.ByteBuffer.allocateDirect(0); continue; }
        java.nio.ByteBuffer d = java.nio.ByteBuffer.allocateDirect(p.length);
        d.put(p).flip();
        direct[i] = d;
      }
      return convert(allocator, direct, thriftPath, structName);
    } else {
      try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
           ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
        decode(payloads, thriftPath, structName, outSchema.memoryAddress(), outArray.memoryAddress());
        return Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
      }
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
    // Prefer the direct ByteBuffer JNI path; copy non-direct buffers into direct ones
    java.nio.ByteBuffer[] direct = new java.nio.ByteBuffer[payloads.length];
    int idx = 0;
    for (java.nio.ByteBuffer bb : payloads) {
      if (bb != null && bb.isDirect()) {
        java.nio.ByteBuffer dup = bb.duplicate(); dup.position(0);
        direct[idx++] = dup.slice();
      } else if (bb != null) {
        java.nio.ByteBuffer dup = bb.duplicate(); dup.position(0);
        java.nio.ByteBuffer d = java.nio.ByteBuffer.allocateDirect(dup.remaining());
        d.put(dup).flip();
        direct[idx++] = d;
      } else {
        direct[idx++] = java.nio.ByteBuffer.allocateDirect(0);
      }
    }
    try (ArrowArray outArray = ArrowArray.allocateNew(allocator);
         ArrowSchema outSchema = ArrowSchema.allocateNew(allocator)) {
      decodeFromDirect(direct, thriftPath, structName, outSchema.memoryAddress(), outArray.memoryAddress());
      return Data.importVectorSchemaRoot(allocator, outArray, outSchema, null);
    }
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
