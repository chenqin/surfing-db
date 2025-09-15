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
    // Ensure libsurfingthriftjni.so is discoverable via java.library.path
    System.loadLibrary("surfingthriftjni");
  }

  private NativeThriftDecoder() {}

  private static native void decode(byte[][] payloads, String thriftPath, String structName,
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
