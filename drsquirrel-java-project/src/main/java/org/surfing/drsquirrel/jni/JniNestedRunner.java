package org.surfing.drsquirrel.jni;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.*;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.StructVector;
import org.apache.arrow.vector.types.pojo.*;

import java.util.*;

/**
 * Standalone runner for nested shuffle/cogroup via JNI under MPI.
 * Usage:
 *   java ... org.surfing.drsquirrel.jni.JniNestedRunner [shuffle|cogroup|all] [two]
 */
public class JniNestedRunner {
    private static Schema schema(boolean left) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field elem = new Field("element", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Field lst = new Field("lst", FieldType.nullable(new ArrowType.List()), Collections.singletonList(elem));
        List<Field> attrsChildren = Arrays.asList(
                new Field("rank", FieldType.nullable(new ArrowType.Int(64, true)), null),
                new Field("idx", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("world", FieldType.nullable(new ArrowType.Int(32, true)), null),
                new Field("side", FieldType.nullable(new ArrowType.Int(32, true)), null)
        );
        Field attrs = new Field("attrs", FieldType.nullable(new ArrowType.Struct()), attrsChildren);
        Field val = new Field(left ? "lval" : "rval", FieldType.nullable(new ArrowType.Int(32, true)), null);
        return new Schema(Arrays.asList(key, lst, attrs, val));
    }
    private static VectorSchemaRoot makeNested(BufferAllocator alloc, boolean left) {
        int rank = NativeProcessors.getMpiRank();
        int world = NativeProcessors.getMpiWorld();
        Schema s = schema(left);
        VectorSchemaRoot root = VectorSchemaRoot.create(s, alloc);
        root.allocateNew();
        BigIntVector key = (BigIntVector) root.getVector("key");
        IntVector val = (IntVector) root.getVector(left ? "lval" : "rval");
        ListVector lst = (ListVector) root.getVector("lst");
        StructVector attrs = (StructVector) root.getVector("attrs");
        IntVector listData = (IntVector) lst.getDataVector();
        int elemIdx = 0;
        for (int i = 0; i < 3; ++i) {
            long k = 1000L * rank + (left ? i : (i + 1));
            key.setSafe(i, k);
            val.setSafe(i, left ? (rank * 10 + i) : (rank * 100 + i));
            lst.startNewValue(i);
            for (int j = 0; j <= i; ++j) listData.setSafe(elemIdx++, j);
            lst.endValue(i, i + 1);
            attrs.setIndexDefined(i);
            ((BigIntVector) attrs.getChild("rank")).setSafe(i, rank);
            ((IntVector) attrs.getChild("idx")).setSafe(i, left ? i : (i + 1));
            ((IntVector) attrs.getChild("world")).setSafe(i, world);
            ((IntVector) attrs.getChild("side")).setSafe(i, left ? 0 : 1);
        }
        key.setValueCount(3); val.setValueCount(3); listData.setValueCount(elemIdx); lst.setValueCount(3); attrs.setValueCount(3);
        root.setRowCount(3);
        return root;
    }
    public static void main(String[] args) throws Exception {
        String which = args.length > 0 ? args[0] : "all";
        boolean two = args.length > 1 && "two".equalsIgnoreCase(args[1]);
        boolean oneSided = !two;
        try (RootAllocator alloc = new RootAllocator()) {
            if ("shuffle".equalsIgnoreCase(which) || "all".equalsIgnoreCase(which)) {
                try (VectorSchemaRoot in = makeNested(alloc, true)) {
                    VectorSchemaRoot out = NativeProcessors.shuffle(alloc, in, "key", oneSided, 0, 0);
                    validateShuffleOut(out, /*left=*/true);
                    System.out.printf("NestedRunner shuffle: rank=%d world=%d rows=%d [OK]\n",
                            NativeProcessors.getMpiRank(), NativeProcessors.getMpiWorld(), out.getRowCount());
                    out.close();
                }
            }
            if ("cogroup".equalsIgnoreCase(which) || "all".equalsIgnoreCase(which)) {
                try (VectorSchemaRoot left = makeNested(alloc, true);
                     VectorSchemaRoot right = makeNested(alloc, false)) {
                    VectorSchemaRoot[] outs = NativeProcessors.cogroup(alloc, left, right, "key", oneSided, 0, 0);
                    validateShuffleOut(outs[0], /*left=*/true);
                    validateShuffleOut(outs[1], /*left=*/false);
                    System.out.printf("NestedRunner cogroup: rank=%d world=%d rowsL=%d rowsR=%d [OK]\n",
                            NativeProcessors.getMpiRank(), NativeProcessors.getMpiWorld(), outs[0].getRowCount(), outs[1].getRowCount());
                    outs[0].close(); outs[1].close();
                }
            }
        }
    }

    private static void validateShuffleOut(VectorSchemaRoot out, boolean left) {
        int world = NativeProcessors.getMpiWorld();
        int rank = NativeProcessors.getMpiRank();

        BigIntVector key = (BigIntVector) out.getVector("key");
        ListVector lst = (ListVector) out.getVector("lst");
        StructVector attrs = (StructVector) out.getVector("attrs");
        IntVector val = (IntVector) out.getVector(left ? "lval" : "rval");

        for (int i = 0; i < out.getRowCount(); ++i) {
            long k = key.get(i);
            int originRank = (int) (k / 1000L);
            int idx = (int) (k % 1000L);
            int expectedVal = left ? (originRank * 10 + idx) : (originRank * 100 + (idx - 1));
            if (val.get(i) != expectedVal) throw new IllegalStateException("val mismatch at row " + i);

            @SuppressWarnings("unchecked")
            List<Integer> seq = (List<Integer>) lst.getObject(i);
            int expectedLen = left ? (idx + 1) : idx;
            if (seq.size() != expectedLen) throw new IllegalStateException("list length mismatch at row " + i);
            for (int j = 0; j < seq.size(); ++j) if (seq.get(j) != j) throw new IllegalStateException("list content mismatch at row " + i);

            BigIntVector rankV = (BigIntVector) attrs.getChild("rank");
            IntVector idxV = (IntVector) attrs.getChild("idx");
            IntVector worldV = (IntVector) attrs.getChild("world");
            IntVector sideV = (IntVector) attrs.getChild("side");
            if (rankV.get(i) != originRank) throw new IllegalStateException("attrs.rank mismatch at row " + i);
            if (idxV.get(i) != idx) throw new IllegalStateException("attrs.idx mismatch at row " + i);
            if (worldV.get(i) != world) throw new IllegalStateException("attrs.world mismatch at row " + i);
            if (sideV.get(i) != (left ? 0 : 1)) throw new IllegalStateException("attrs.side mismatch at row " + i);

            // Heuristic partition check: for int64 keys constructed as originRank*1000+idx,
            // value % world should equal native rank if hashing preserves modulo.
            if ((k % world) != rank) {
                // non-fatal: only warn due to potential hash mixing differences
                System.err.printf("[warn] partition heuristic failed: key=%d world=%d rank=%d\n", k, world, rank);
            }
        }
    }
}
