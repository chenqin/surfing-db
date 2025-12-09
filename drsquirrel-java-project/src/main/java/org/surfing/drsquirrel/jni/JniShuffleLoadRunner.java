package org.surfing.drsquirrel.jni;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.util.Arrays;
import java.util.Random;

/**
 * MPI Java load runner for JNI processors shuffle.
 *
 * Usage (run under mpiexec):
 *   mpiexec -np 4 java -Djava.library.path=$PWD/build \
 *     -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *     org.surfing.drsquirrel.jni.JniShuffleLoadRunner [two]
 *
 * Env:
 *   SHUFFLE_LOAD_ROWS: rows per rank (default 200000)
 *   SHUFFLE_LOAD_ITERS: iterations (default 3)
 *   SHUFFLE_TEST_SEED: base random seed (optional)
 *   SHUFFLE_LOAD_OUT: CSV path to append results (rank 0 only)
 */
public class JniShuffleLoadRunner {
    private static long getEnvLong(String name, long defVal) {
        String v = System.getenv(name);
        if (v == null) return defVal;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return defVal; }
    }

    private static String getEnv(String name) {
        String v = System.getenv(name);
        return v == null ? "" : v;
    }

    private static int getOmniRank() {
        String s = System.getenv("OMPI_COMM_WORLD_RANK");
        if (s == null) s = System.getenv("PMI_RANK");
        if (s == null) return 0;
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { return 0; }
    }

    private static int getOmniWorld() {
        String s = System.getenv("OMPI_COMM_WORLD_SIZE");
        if (s == null) s = System.getenv("PMI_SIZE");
        if (s == null) return 1;
        try { return Integer.parseInt(s); } catch (NumberFormatException e) { return 1; }
    }

    private static VectorSchemaRoot makeRandomBatch(BufferAllocator alloc, long rows, long seed) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field val = new Field("val", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(key, val));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        BigIntVector keyVec = (BigIntVector) root.getVector("key");
        IntVector valVec = (IntVector) root.getVector("val");
        root.allocateNew();

        Random rnd = new Random(seed);
        for (int i = 0; i < rows; i++) {
            long k = rnd.nextLong();
            keyVec.setSafe(i, k);
            valVec.setSafe(i, i);
        }
        keyVec.setValueCount((int) rows);
        valVec.setValueCount((int) rows);
        root.setRowCount((int) rows);
        return root;
    }

    public static void main(String[] args) throws IOException {
        boolean twoSided = args.length > 0 && "two".equalsIgnoreCase(args[0]);
        long rowsPerRank = getEnvLong("SHUFFLE_LOAD_ROWS", 200_000);
        long iters = getEnvLong("SHUFFLE_LOAD_ITERS", 3);
        String outCsv = getEnv("SHUFFLE_LOAD_OUT");
        long seedBase = getEnvLong("SHUFFLE_TEST_SEED", System.currentTimeMillis());

        int rank = getOmniRank();
        int world = getOmniWorld();
        String mode = twoSided ? "two-sided" : "one-sided";

        FileWriter csv = null;
        boolean csvEnabled = false;
        if (!outCsv.isEmpty() && rank == 0) {
            csv = new FileWriter(outCsv, true);
            csv.write("run_id,ts_epoch,mode,world,rows_per_rank,iter,rows,time_s,rows_per_sec\n");
            csv.flush();
            csvEnabled = true;
        }

        if (rank == 0) {
            System.out.println("[JniLoadTest] world=" + world +
                    " rows_per_rank=" + rowsPerRank +
                    " iters=" + iters +
                    " mode=" + mode);
        }

        try (RootAllocator alloc = new RootAllocator()) {
            try (VectorSchemaRoot in = makeRandomBatch(alloc, rowsPerRank, seedBase + rank)) {
                // warmup
                VectorSchemaRoot warm = NativeProcessors.shuffle(alloc, in, "key", !twoSided, rank, world);
                if (warm != null) warm.close();

                double best = Double.MAX_VALUE, total = 0.0;
                for (int i = 0; i < iters; i++) {
                    long t0 = System.nanoTime();
                    VectorSchemaRoot out = NativeProcessors.shuffle(alloc, in, "key", !twoSided, rank, world);
                    long t1 = System.nanoTime();
                    if (out != null) out.close();
                    double dt = (t1 - t0) / 1e9;
                    best = Math.min(best, dt);
                    total += dt;

                    long rows = rowsPerRank * world; // expected global rows
                    double rps = rows / dt;
                    if (rank == 0) {
                        System.out.println("[JniLoadTest] iter=" + i +
                                " rows=" + rows +
                                " time_s=" + dt +
                                " rows_per_sec=" + rps);
                        if (csvEnabled) {
                            long epoch = Instant.now().getEpochSecond();
                            csv.write(epoch + "," + epoch + "," + mode + "," + world + "," + rowsPerRank + "," + i + "," + rows + "," + dt + "," + rps + "\n");
                            csv.flush();
                        }
                    }
                }
                if (rank == 0) {
                    double avg = total / (double) iters;
                    System.out.println("[JniLoadTest] best_s=" + best + " avg_s=" + avg);
                }
            }
        }

        if (csvEnabled) {
            csv.close();
        }
    }
}

