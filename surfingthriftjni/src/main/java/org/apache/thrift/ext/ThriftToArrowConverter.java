package org.apache.thrift.ext;

import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.thrift.TBase;

/**
 * Converts Thrift-serialized payloads into Apache Arrow batches.
 *
 * Usage pattern:
 * - Provide the generated Thrift class (e.g., com.example.thrift.MyStruct.class)
 * - Provide one or more payloads as byte[] arrays (Binary protocol)
 * - The converter derives an Arrow Schema from Thrift metadata and materializes rows
 */
public interface ThriftToArrowConverter {

  /**
   * Infer Arrow schema from a Thrift struct class via Thrift metadata.
   */
  Schema toArrowSchema(Class<? extends TBase<?, ?>> thriftClass);

  /**
   * Convert a single Thrift payload (Binary protocol) to a single-row Arrow batch.
   * Caller owns the returned root and must close it.
   */
  VectorSchemaRoot convert(byte[] payload,
                           Class<? extends TBase<?, ?>> thriftClass,
                           BufferAllocator allocator);

  /**
   * Convert many Thrift payloads into a multi-row Arrow batch.
   * Caller owns the returned root and must close it.
   */
  VectorSchemaRoot convert(List<byte[]> payloads,
                           Class<? extends TBase<?, ?>> thriftClass,
                           BufferAllocator allocator);
}

