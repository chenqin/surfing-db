package org.surfing.drsquirrel.arrow;

import org.surfing.drsquirrel.jni.NativeThriftDecoder;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Flink map function that decodes DeepEvent Thrift payloads into Arrow-backed views via
 * {@link NativeThriftDecoder}.
 */
public final class ArrowDeepEventDecoder extends RichMapFunction<byte[], DeepEventView> {

  private final String thriftPath;
  private final String structName;
  private transient BufferAllocator allocator;

  public ArrowDeepEventDecoder(String thriftPath, String structName) {
    this.thriftPath = thriftPath;
    this.structName = structName;
  }

  @Override
  public void open(Configuration parameters) {
    allocator = new RootAllocator();
  }

  @Override
  public DeepEventView map(byte[] value) throws Exception {
    VectorSchemaRoot root = NativeThriftDecoder.convert(allocator, new byte[][]{value}, thriftPath, structName);
    return new ArrowDeepEventView(root, 0);
  }

  @Override
  public void close() {
    if (allocator != null) {
      allocator.close();
      allocator = null;
    }
  }
}
