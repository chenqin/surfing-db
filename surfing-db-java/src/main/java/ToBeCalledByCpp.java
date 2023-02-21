import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;

import java.util.Arrays;

public class ToBeCalledByCpp {
    final static BufferAllocator allocator = new RootAllocator();

    /**
     * Create a {@link FieldVector} and export it via the C Data Interface
     * 
     * @param schemaAddress Schema memory address to wrap
     * @param arrayAddress  Array memory address to wrap
     */
    public static void fillVector(long schemaAddress, long arrayAddress) {
        try (ArrowArray arrow_array = ArrowArray.wrap(arrayAddress);
                ArrowSchema arrow_schema = ArrowSchema.wrap(schemaAddress)) {
            Data.exportVector(allocator, populateFieldVectorToExport(), null, arrow_array, arrow_schema);
        }
    }

    /**
     * Create a {@link VectorSchemaRoot} and export it via the C Data Interface
     * 
     * @param schemaAddress Schema memory address to wrap
     * @param arrayAddress  Array memory address to wrap
     */
    public static void fillVectorSchemaRoot(long schemaIn, long arrayIn, long schemaOut, long arrayOut) {
        try (ArrowArray array_in = ArrowArray.wrap(arrayIn);
                ArrowSchema schema_in = ArrowSchema.wrap(schemaIn); 
                ArrowArray array_out = ArrowArray.wrap(arrayOut);
                ArrowSchema schema_out = ArrowSchema.wrap(schemaOut)) {
            VectorSchemaRoot root = Data.importVectorSchemaRoot(allocator, array_in, schema_in, null);
            System.out.println(root.toString());
            /**
             * user code
             */
            Data.exportVectorSchemaRoot(allocator, populateVectorSchemaRootToExport(), null, array_out, schema_out);
        }
    }

    private static FieldVector populateFieldVectorToExport() {
        IntVector intVector = new IntVector("int-to-export", allocator);
        intVector.allocateNew(3);
        intVector.setSafe(0, 1);
        intVector.setSafe(1, 2);
        intVector.setSafe(2, 3);
        intVector.setValueCount(3);
        System.out.println("[Java] FieldVector: \n" + intVector);
        return intVector;
    }

    private static VectorSchemaRoot populateVectorSchemaRootToExport() {
        IntVector intVector = new IntVector("age-to-export", allocator);
        intVector.setSafe(0, 10);
        intVector.setSafe(1, 20);
        intVector.setSafe(2, 30);
        VectorSchemaRoot root = new VectorSchemaRoot(Arrays.asList(intVector));
        root.setRowCount(3);
        System.out.println("[Java] VectorSchemaRoot: \n" + root.contentToTSVString());
        return root;
    }
}