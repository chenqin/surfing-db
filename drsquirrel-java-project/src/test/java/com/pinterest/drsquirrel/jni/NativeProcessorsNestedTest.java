package com.pinterest.drsquirrel.jni;

import junit.framework.TestCase;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.*;

public class NativeProcessorsNestedTest extends TestCase {

    private Schema makeNestedSchema(boolean leftSide) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field listElem = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field lst = new Field("lst", FieldType.nullable(new ArrowType.List()), Collections.singletonList(listElem));
        List<Field> attrsChildren = Arrays.asList(
                new Field("rank", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("idx", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("world", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("side", FieldType.nullable(new ArrowType.Int(32, true)), null)
        );
        Field attrs = new Field("attrs", FieldType.nullable(new ArrowType.Struct()), attrsChildren);
        Field val = new Field(leftSide ? "lval" : "rval", FieldType.nullable(new ArrowType.Int(32, true)), null);
        return new Schema(Arrays.asList(key, lst, attrs, val));
    }

    private VectorSchemaRoot makeNestedBatch(BufferAllocator alloc, boolean leftSide, int world) {
        Schema schema = makeNestedSchema(leftSide);
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        root.allocateNew();

        BigIntVector keyVec = (BigIntVector) root.getVector("key");
        IntVector valVec = (IntVector) root.getVector(leftSide ? "lval" : "rval");
        ListVector lst = (ListVector) root.getVector("lst");
        StructVector attrs = (StructVector) root.getVector("attrs");

        int rank = 0; // single-JVM, MPI world=1 in native
        int elemIndex = 0;
        IntVector listData = (IntVector) lst.getDataVector();
        for (int i = 0; i < 3; ++i) {
            long key = 1000L * rank + (leftSide ? i : (i + 1));
            keyVec.setSafe(i, key);
            int val = leftSide ? (rank * 10 + i) : (rank * 100 + i);
            valVec.setSafe(i, val);

            // lst = [0, 1, ..., i]
            lst.startNewValue(i);
            for (int j = 0; j <= i; ++j) {
                listData.setSafe(elemIndex++, j);
            }
            lst.endValue(i, i + 1);

            // attrs struct: mark valid and set children
            attrs.setIndexDefined(i);
            ((BigIntVector) attrs.getChild("rank")).setSafe(i, rank);
            ((IntVector) attrs.getChild("idx")).setSafe(i, leftSide ? i : (i + 1));
            ((IntVector) attrs.getChild("world")).setSafe(i, world);
            ((IntVector) attrs.getChild("side")).setSafe(i, leftSide ? 0 : 1);
        }
        keyVec.setValueCount(3);
        valVec.setValueCount(3);
        listData.setValueCount(elemIndex);
        lst.setValueCount(3);
        attrs.setValueCount(3);
        root.setRowCount(3);
        return root;
    }

    public void testShuffleNestedOneSided() {
        try (RootAllocator alloc = new RootAllocator()) {
            try (VectorSchemaRoot in = makeNestedBatch(alloc, true, 4)) {
                try {
                    VectorSchemaRoot out = NativeProcessors.shuffle(alloc, in, "key", true, 0, 4);
                    assertNotNull(out);
                    assertEquals(3, out.getRowCount());
                    // Validate nested contents preserved in single-rank run
                    ListVector lst = (ListVector) out.getVector("lst");
                    StructVector attrs = (StructVector) out.getVector("attrs");
                    BigIntVector key = (BigIntVector) out.getVector("key");
                    IntVector lval = (IntVector) out.getVector("lval");
                    for (int i = 0; i < 3; ++i) {
                        assertEquals((long) i, key.get(i));
                        assertEquals(i, lval.get(i));
                        @SuppressWarnings("unchecked")
                        List<Integer> seq = (List<Integer>) lst.getObject(i);
                        assertEquals(i + 1, seq.size());
                        for (int j = 0; j < seq.size(); ++j) assertEquals(Integer.valueOf(j), seq.get(j));
                        BigIntVector rankV = (BigIntVector) attrs.getChild("rank");
                        IntVector idxV = (IntVector) attrs.getChild("idx");
                        IntVector worldV = (IntVector) attrs.getChild("world");
                        IntVector sideV = (IntVector) attrs.getChild("side");
                        assertEquals(0L, rankV.get(i));
                        assertEquals(i, idxV.get(i));
                        assertEquals(4, worldV.get(i));
                        assertEquals(0, sideV.get(i));
                    }
                    out.close();
                } catch (UnsatisfiedLinkError e) {
                    System.out.println("[SKIP] NativeProcessorsNestedTest.shuffle: JNI lib not found");
                }
            }
        }
    }

    public void testCogroupNestedOneSided() {
        try (RootAllocator alloc = new RootAllocator()) {
            try (VectorSchemaRoot left = makeNestedBatch(alloc, true, 4);
                 VectorSchemaRoot right = makeNestedBatch(alloc, false, 4)) {
                try {
                    VectorSchemaRoot[] outs = NativeProcessors.cogroup(alloc, left, right, "key", true, 0, 4);
                    assertNotNull(outs);
                    assertEquals(2, outs.length);
                    assertEquals(3, outs[0].getRowCount());
                    assertEquals(3, outs[1].getRowCount());
                    // Spot check a row on each side
                    StructVector lattrs = (StructVector) outs[0].getVector("attrs");
                    IntVector lval = (IntVector) outs[0].getVector("lval");
                    assertEquals(2, lval.get(2));
                    assertEquals(2, ((IntVector) lattrs.getChild("idx")).get(2));

                    StructVector rattrs = (StructVector) outs[1].getVector("attrs");
                    IntVector rval = (IntVector) outs[1].getVector("rval");
                    assertEquals(2, rval.get(2));
                    assertEquals(3, ((IntVector) rattrs.getChild("idx")).get(2));

                    outs[0].close();
                    outs[1].close();
                } catch (UnsatisfiedLinkError e) {
                    System.out.println("[SKIP] NativeProcessorsNestedTest.cogroup: JNI lib not found");
                }
            }
        }
    }
}
