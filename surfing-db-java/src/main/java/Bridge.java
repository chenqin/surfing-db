import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * user expect to override Bridge and implement thier own process
 * function
 */
public class Bridge {
  protected static final BufferAllocator allocator = new RootAllocator();
  protected static final Logger LOG = LoggerFactory.getLogger(Bridge.class);
  protected static long total = 0;

  /**
   * Create a {@link VectorSchemaRoot} and export it via the C Data Interface
   *
   * @param schemaAddress Schema memory address to wrap
   * @param arrayAddress Array memory address to wrap
   */
  public static void _internal_invoke(long schemaIn, long arrayIn, long schemaOut, long arrayOut) {
    try (ArrowArray array_in = ArrowArray.wrap(arrayIn);
        ArrowSchema schema_in = ArrowSchema.wrap(schemaIn);
        ArrowArray array_out = ArrowArray.wrap(arrayOut);
        ArrowSchema schema_out = ArrowSchema.wrap(schemaOut)) {
      VectorSchemaRoot input = Data.importVectorSchemaRoot(allocator, array_in, schema_in, null);
      try {
        Data.exportVectorSchemaRoot(allocator, process(input), null, array_out, schema_out);
      } catch (Exception e) {

      } finally {
        input.clear();
      }
    }
  }

  /**
   * user defined function entry with zero copy
   *
   * @param arrow memory pointer passing from c++
   * @return arrow memory pointer passing back to c++
   */
  protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
    total += input.getRowCount();
    BitVector bitVector = new BitVector("boolean", allocator);
    VarCharVector varCharVector = new VarCharVector("varchar", allocator);
    bitVector.allocateNew();
    varCharVector.allocateNew();
    for (int i = 0; i < 10; i++) {
      bitVector.setSafe(i, i % 2 == 0 ? 0 : 1);
      varCharVector.setSafe(i, ("test" + i).getBytes(StandardCharsets.UTF_8));
    }
    bitVector.setValueCount(10);
    varCharVector.setValueCount(10);
    List<Field> fields = Arrays.asList(bitVector.getField(), varCharVector.getField());
    List<FieldVector> vectors = Arrays.asList(bitVector, varCharVector);
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(fields, vectors);
    return vectorSchemaRoot;
  }
}
