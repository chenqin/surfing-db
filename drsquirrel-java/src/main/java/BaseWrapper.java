import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseWrapper {
    protected final BufferAllocator allocator;
    protected final ObjectMapper mapper;
    protected final Logger LOG = LoggerFactory.getLogger(BaseWrapper.class);

    public BaseWrapper() {
        allocator = new RootAllocator();
        mapper = new ObjectMapper();
    }

    public void CppInvoke(long schemaIn, long arrayIn, long schemaOut, long arrayOut) {
        try (ArrowArray array_in = ArrowArray.wrap(arrayIn);
             ArrowSchema schema_in = ArrowSchema.wrap(schemaIn);
             ArrowArray array_out = ArrowArray.wrap(arrayOut);
             ArrowSchema schema_out = ArrowSchema.wrap(schemaOut)) {
            VectorSchemaRoot input = null;
            VectorSchemaRoot output = null;
            try {
                input = Data.importVectorSchemaRoot(allocator, array_in, schema_in, null);
                output = process(input);
                Data.exportVectorSchemaRoot(allocator, output, null, array_out, schema_out);
            } catch (Exception e) {
                LOG.error(e.getMessage());
            } finally {
                if (input != null)
                    input.close();
                if (output != null)
                    output.close();
                System.gc(); // Notice, user need to do gc to avoid memory leak
            }
        }
    }

    public abstract VectorSchemaRoot process(VectorSchemaRoot input) throws Exception;
}
