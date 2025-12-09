package org.surfing.drsquirrel.jni;

import junit.framework.TestCase;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;

public class CogroupTest extends TestCase {
    private VectorSchemaRoot makeBatch(BufferAllocator alloc, String valName, int base) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field val = new Field(valName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(key, val));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);

        BigIntVector keyVec = (BigIntVector) root.getVector("key");
        IntVector valVec = (IntVector) root.getVector(valName);
        root.allocateNew();
        keyVec.setSafe(0, 1);
        valVec.setSafe(0, base + 10);
        keyVec.setSafe(1, 2);
        valVec.setSafe(1, base + 20);
        keyVec.setValueCount(2);
        valVec.setValueCount(2);
        root.setRowCount(2);
        return root;
    }

    public void testCogroupOneSided() {
        try (RootAllocator alloc = new RootAllocator()) {
            try (VectorSchemaRoot left = makeBatch(alloc, "la", 0);
                 VectorSchemaRoot right = makeBatch(alloc, "rb", 100)) {
                try {
                    VectorSchemaRoot[] outs = NativeProcessors.cogroup(alloc, left, right, "key", true, 0, 1);
                    assertNotNull(outs);
                    assertEquals(2, outs.length);
                    assertEquals(2, outs[0].getRowCount());
                    assertEquals(2, outs[1].getRowCount());
                    outs[0].close();
                    outs[1].close();
                } catch (UnsatisfiedLinkError e) {
                    System.out.println("[SKIP] CogroupTest: JNI lib not found");
                }
            }
        }
    }
}
