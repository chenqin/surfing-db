import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * user expect to override Bridge and implement thier own process function
 */
public class Bridge {
    static long total = 0;
    protected final static BufferAllocator allocator = new RootAllocator();
    /**
     * Create a {@link VectorSchemaRoot} and export it via the C Data Interface
     * 
     * @param schemaAddress Schema memory address to wrap
     * @param arrayAddress  Array memory address to wrap
     */
    public static void _internal_invoke(long schemaIn, long arrayIn, long schemaOut, long arrayOut) {
        try (ArrowArray array_in = ArrowArray.wrap(arrayIn);
                ArrowSchema schema_in = ArrowSchema.wrap(schemaIn); 
                ArrowArray array_out = ArrowArray.wrap(arrayOut);
                ArrowSchema schema_out = ArrowSchema.wrap(schemaOut)) {
            VectorSchemaRoot input = Data.importVectorSchemaRoot(allocator, array_in, schema_in, null);
            Data.exportVectorSchemaRoot(allocator, process(input), null, array_out, schema_out);
        }
    }
    
    /**
     * user defined function entry with zero copy
     * @param arrow memory pointer passing from c++
     * @return arrow memory pointer passing back to c++
     */
    protected static VectorSchemaRoot process(VectorSchemaRoot input) {
        total += input.getRowCount();
        System.out.println(total);
        return input;
    }
}