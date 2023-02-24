import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;


import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;

/**
 * my bridge example
 */
public class MyBridge extends Bridge {
    /**
     * user defined function entry with zero copy
     * 
     * @param arrow memory pointer passing from c++
     * @return arrow memory pointer passing back to c++
     */
    protected static VectorSchemaRoot process(VectorSchemaRoot input) {
        total += input.getRowCount();
        System.out.println(total);

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