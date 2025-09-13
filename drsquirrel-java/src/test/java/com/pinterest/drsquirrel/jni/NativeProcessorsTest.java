package com.pinterest.drsquirrel.jni;

import junit.framework.TestCase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.Arrays;

public class NativeProcessorsTest extends TestCase {

    private VectorSchemaRoot makeBatch(BufferAllocator alloc) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field val = new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(key, val));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);

        try (BigIntVector keyVec = (BigIntVector) root.getVector("key");
             IntVector valVec = (IntVector) root.getVector("val")) {
            root.allocateNew();
            // 3 rows
            keyVec.setSafe(0, 1);
            valVec.setSafe(0, 10);
            keyVec.setSafe(1, 2);
            valVec.setSafe(1, 20);
            keyVec.setSafe(2, 3);
            valVec.setSafe(2, 30);
            keyVec.setValueCount(3);
            valVec.setValueCount(3);
            root.setRowCount(3);
        }
        return root;
    }

    public void testShuffleOneSided() {
        // JNI lib is loaded by NativeProcessors class static initializer.
        try (RootAllocator alloc = new RootAllocator()) {
            try (VectorSchemaRoot in = makeBatch(alloc)) {
                VectorSchemaRoot out = NativeProcessors.shuffle(alloc, in, "key", true, 0, 1);
                assertNotNull(out);
                assertEquals(3, out.getRowCount());
                out.close();
            }
        }
    }
}
