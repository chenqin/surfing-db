package com.example.fakecogroup;

import com.pinterest.drsquirrel.jni.CogroupInputFactory;
import com.pinterest.drsquirrel.jni.NativeProcessors;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.commons.cli.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

/**
 * Fake source/sink example that co-groups two generated record batches by key under MPI.
 *
 * Usage with mpiexec (from repo root):
 *   mpiexec -np 4 --use-hwthread-cpus --oversubscribe --map-by core --bind-to core \
 *     -x LD_LIBRARY_PATH=$PWD/build:$LD_LIBRARY_PATH \
 *     java -Djava.library.path=$PWD/build \
 *       -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar:examples/fake-cogroup-app/target/fake-cogroup-app-0.1.0-SNAPSHOT.jar \
 *       com.example.fakecogroup.Main --mode one --rows 100000 --iters 1 --out build/examples/fake-cogroup-out
 */
public class Main {
    private static int omniRank() {
        String s = System.getenv("OMPI_COMM_WORLD_RANK");
        if (s == null) s = System.getenv("PMI_RANK");
        if (s == null) return 0;
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { return 0; }
    }
    private static int omniWorld() {
        String s = System.getenv("OMPI_COMM_WORLD_SIZE");
        if (s == null) s = System.getenv("PMI_SIZE");
        if (s == null) return 1;
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { return 1; }
    }

    private static VectorSchemaRoot makeRandomBatch(BufferAllocator alloc, int rows, long seed, String valName) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field val = new Field(valName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(key, val));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        BigIntVector keyVec = (BigIntVector) root.getVector("key");
        IntVector valVec = (IntVector) root.getVector(valName);
        root.allocateNew();
        Random rnd = new Random(seed);
        for (int i = 0; i < rows; i++) {
            keyVec.setSafe(i, rnd.nextLong());
            valVec.setSafe(i, i);
        }
        keyVec.setValueCount(rows);
        valVec.setValueCount(rows);
        root.setRowCount(rows);
        return root;
    }

    private static Options opts() {
        Options o = new Options();
        o.addOption(Option.builder().longOpt("mode").hasArg().desc("one | two (default: one)").build());
        o.addOption(Option.builder().longOpt("rows").hasArg().desc("rows per rank (default 100000)").build());
        o.addOption(Option.builder().longOpt("iters").hasArg().desc("iterations (default 1)").build());
        o.addOption(Option.builder().longOpt("out").hasArg().desc("output directory for fake sink (default build/examples/fake-cogroup-out)").build());
        o.addOption(Option.builder().longOpt("sort-by").hasArg().desc("optional: field name to sort outputs by after cogroup").build());
        o.addOption(Option.builder().longOpt("thrift-path").hasArg().desc("Path to Thrift IDL when decoding payloads via surfingthriftjni").build());
        o.addOption(Option.builder().longOpt("thrift-struct").hasArg().desc("Struct name in the Thrift IDL").build());
        o.addOption(Option.builder().longOpt("payload-left").hasArg().desc("Left payload file (little-endian [len][bytes] format)").build());
        o.addOption(Option.builder().longOpt("payload-right").hasArg().desc("Right payload file (defaults to left if omitted)").build());
        o.addOption(Option.builder().longOpt("key-field").hasArg().desc("Key field for cogroup (default: key; DeepEvent uses event_id)").build());
        return o;
    }

    public static void main(String[] args) throws Exception {
        CommandLineParser parser = new DefaultParser();
        CommandLine cl;
        try {
            cl = parser.parse(opts(), args);
        } catch (ParseException e) {
            new HelpFormatter().printHelp("fake-cogroup-app", opts());
            System.err.println("Parse error: " + e.getMessage());
            System.exit(2);
            return;
        }

        String mode = cl.getOptionValue("mode", "one");
        boolean oneSided = !"two".equalsIgnoreCase(mode);
        int rows = Integer.parseInt(cl.getOptionValue("rows", "100000"));
        int iters = Integer.parseInt(cl.getOptionValue("iters", "1"));
        String outDirS = cl.getOptionValue("out", "build/examples/fake-cogroup-out");
        String sortBy = cl.getOptionValue("sort-by", "");
        String thriftPath = cl.getOptionValue("thrift-path", System.getenv("SHUFFLE_THRIFT_PATH"));
        String thriftStruct = cl.getOptionValue("thrift-struct", System.getenv("SHUFFLE_THRIFT_STRUCT"));
        String payloadLeft = cl.getOptionValue("payload-left", System.getenv("SHUFFLE_THRIFT_PAYLOAD_LEFT"));
        String payloadRight = cl.getOptionValue("payload-right", System.getenv("SHUFFLE_THRIFT_PAYLOAD_RIGHT"));
        boolean useThrift = thriftPath != null && !thriftPath.isEmpty()
                && thriftStruct != null && !thriftStruct.isEmpty()
                && payloadLeft != null && !payloadLeft.isEmpty();
        String keyField = cl.getOptionValue("key-field");
        if (keyField == null || keyField.isEmpty()) {
            String envKey = System.getenv("SHUFFLE_THRIFT_KEY_FIELD");
            if (envKey != null && !envKey.isEmpty()) {
                keyField = envKey;
            } else if (useThrift && "DeepEvent".equals(thriftStruct)) {
                keyField = "event_id";
            } else {
                keyField = "key";
            }
        }
        File outDir = new File(outDirS);
        if (!outDir.exists() && !outDir.mkdirs() && !outDir.exists()) {
            throw new IOException("Failed to create output dir: " + outDir);
        }

        int rank = omniRank();
        int world = omniWorld();
        long seedBase = Instant.now().getEpochSecond();
        try (RootAllocator alloc = new RootAllocator()) {
            CogroupInputFactory.ThriftInputs thriftInputs = null;
            VectorSchemaRoot left = null;
            VectorSchemaRoot right = null;
            long rowsPerRankActual = rows;
            try {
                if (useThrift) {
                    thriftInputs = CogroupInputFactory.fromThrift(
                            alloc,
                            thriftPath,
                            thriftStruct,
                            payloadLeft,
                            payloadRight == null || payloadRight.isEmpty() ? null : payloadRight,
                            keyField,
                            rank,
                            world);
                    left = thriftInputs.left;
                    right = thriftInputs.right;
                    keyField = thriftInputs.keyField;
                    rowsPerRankActual = thriftInputs.rowsPerRank;
                } else {
                    left = makeRandomBatch(alloc, rows, seedBase + rank, "la");
                    right = makeRandomBatch(alloc, rows, seedBase + rank + 17, "rb");
                }

                if (rank == 0) {
                    System.out.printf("[fake-cogroup] world=%d rows_per_rank=%d iters=%d mode=%s out=%s\n",
                            world, rowsPerRankActual, iters, (oneSided ? "one" : "two"), outDir.getAbsolutePath());
                    if (useThrift) {
                        System.out.printf("[fake-cogroup] thrift schema=%s struct=%s key=%s left=%s%s\n",
                                thriftPath, thriftStruct, keyField, payloadLeft,
                                (payloadRight == null || payloadRight.isEmpty()) ? "" : (" right=" + payloadRight));
                    }
                }

                // Warmup
                VectorSchemaRoot[] warm = NativeProcessors.cogroup(alloc, left, right, keyField, oneSided, rank, world);
                if (warm != null) { for (VectorSchemaRoot v : warm) if (v != null) v.close(); }

                double best = Double.MAX_VALUE, total = 0.0;
                for (int i = 0; i < iters; i++) {
                    long t0 = System.nanoTime();
                    VectorSchemaRoot[] outs = NativeProcessors.cogroup(alloc, left, right, keyField, oneSided, rank, world);
                    long t1 = System.nanoTime();
                    double dt = (t1 - t0) / 1e9;
                    best = Math.min(best, dt);
                    total += dt;

                    // Fake sink: write a tiny per-rank sample and metrics
                    VectorSchemaRoot leftOut = outs[0];
                    VectorSchemaRoot rightOut = outs[1];
                    if (!sortBy.isEmpty()) {
                        if (leftOut.getSchema().findField(sortBy) != null) {
                            VectorSchemaRoot sorted = sortByField(alloc, leftOut, sortBy);
                            leftOut.close();
                            leftOut = sorted;
                        }
                        if (rightOut.getSchema().findField(sortBy) != null) {
                            VectorSchemaRoot sorted = sortByField(alloc, rightOut, sortBy);
                            rightOut.close();
                            rightOut = sorted;
                        }
                    }

                    int rowsL = leftOut.getRowCount();
                    int rowsR = rightOut.getRowCount();
                    File sample = new File(outDir, String.format("rank-%d-iter-%d-sample.txt", rank, i));
                    try (FileWriter fw = new FileWriter(sample)) {
                        fw.write(String.format("rank=%d world=%d rowsL=%d rowsR=%d time_s=%.6f mode=%s\n",
                                rank, world, rowsL, rowsR, dt, oneSided ? "one" : "two"));
                        // Up to 5 rows from each side
                        if (!sortBy.isEmpty()) fw.write("sorted_by=" + sortBy + "\n");
                        dumpSample(leftOut, "left", fw, 5);
                        dumpSample(rightOut, "right", fw, 5);
                    }
                    leftOut.close();
                    rightOut.close();
                    for (VectorSchemaRoot v : outs) if (v != null) v.close();

                    if (rank == 0) {
                        long totalRows = rowsPerRankActual * world * 2;
                        double rps = totalRows / dt;
                        System.out.printf("[fake-cogroup] iter=%d rows=%d time_s=%.6f rows_per_sec=%.0f\n",
                                i, totalRows, dt, rps);
                    }
                }
                if (rank == 0) {
                    double avg = total / (double) iters;
                    System.out.printf("[fake-cogroup] best_s=%.6f avg_s=%.6f\n", best, avg);
                }
            } finally {
                if (useThrift) {
                    if (thriftInputs != null) {
                        thriftInputs.close();
                    }
                } else {
                    if (left != null) left.close();
                    if (right != null) right.close();
                }
            }
        }
    }

    private static void dumpSample(VectorSchemaRoot root, String label, FileWriter fw, int maxRows) throws IOException {
        int n = Math.min(maxRows, root.getRowCount());
        fw.write("sample_" + label + " (first " + n + ")\n");
        List<FieldVector> vectors = root.getFieldVectors();
        for (int i = 0; i < n; i++) {
            fw.write("  " + i + ": ");
            for (int f = 0; f < vectors.size(); f++) {
                FieldVector vec = vectors.get(f);
                Object value = vec.getObject(i);
                fw.write(vec.getName() + "=" + value);
                if (f + 1 < vectors.size()) {
                    fw.write(", ");
                }
            }
            fw.write("\n");
        }
    }

    private static VectorSchemaRoot sortByField(BufferAllocator alloc, VectorSchemaRoot root, String fieldName) {
        int n = root.getRowCount();
        Integer[] idx = new Integer[n];
        for (int i = 0; i < n; i++) idx[i] = i;

        // Determine comparator based on vector type
        final IntVector sortIntVec = (IntVector) root.getVector(fieldName);
        final BigIntVector sortLongVec = sortIntVec == null ? (BigIntVector) root.getVector(fieldName) : null;

        if (sortIntVec == null && sortLongVec == null) {
            throw new IllegalArgumentException("sort field not int32 or int64: " + fieldName);
        }

        java.util.Arrays.sort(idx, (a, b) -> {
            if (sortIntVec != null) {
                int va = sortIntVec.get(a);
                int vb = sortIntVec.get(b);
                return Integer.compare(va, vb);
            } else {
                long va = sortLongVec.get(a);
                long vb = sortLongVec.get(b);
                return Long.compare(va, vb);
            }
        });

        // Build a new root and copy rows in sorted order
        VectorSchemaRoot out = VectorSchemaRoot.create(root.getSchema(), alloc);
        out.allocateNew();
        for (int col = 0; col < root.getFieldVectors().size(); col++) {
            org.apache.arrow.vector.ValueVector src = root.getVector(col);
            org.apache.arrow.vector.ValueVector dst = out.getVector(col);
            for (int i = 0; i < n; i++) {
                dst.copyFromSafe(idx[i].intValue(), i, src);
            }
            dst.setValueCount(n);
        }
        out.setRowCount(n);
        return out;
    }
}
