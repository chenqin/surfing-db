package com.pinterest.drsquirrel.jni;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
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
 * MPI Java load runner for JNI processors cogroup.
 * Produces two inputs per rank and co-shuffles them by the same key.
 *
 * Usage (under mpiexec):
 *   mpiexec -np 4 java -Djava.library.path=$PWD/build \
 *     -cp drsquirrel-java-project/target/drsquirrel-java-1.0-SNAPSHOT-jar-with-dependencies.jar \
 *     com.pinterest.drsquirrel.jni.JniCogroupLoadRunner [two]
 *
 * Env:
 *   SHUFFLE_LOAD_ROWS: rows per rank (default 200000)
 *   SHUFFLE_LOAD_ITERS: iterations (default 3)
 *   SHUFFLE_TEST_SEED: base random seed (optional)
 *   SHUFFLE_LOAD_OUT: CSV path to append results (rank 0 only)
 *   COGROUP_THRIFT_FILE: optional thrift IDL path (enables thrift decoding)
 *   COGROUP_THRIFT_STRUCT: thrift struct name
 *   COGROUP_THRIFT_PAYLOAD: payload file (left input). If *_LEFT omitted, uses this value
 *   COGROUP_THRIFT_PAYLOAD_LEFT / _RIGHT: explicit payloads for each side
 *   COGROUP_KEY_FIELD: key column name (default "key")
 */
public class JniCogroupLoadRunner {
    private static long getEnvLong(String name, long defVal) {
        String v = System.getenv(name);
        if (v == null) return defVal;
        try { return Long.parseLong(v); } catch (NumberFormatException e) { return defVal; }
    }

    private static String getEnv(String name) {
        return getEnv(name, "");
    }

    private static String getEnv(String name, String defVal) {
        String v = System.getenv(name);
        return v == null ? defVal : v;
    }

    private static String requireArg(String[] args, int index, String flag) {
        if (index >= args.length) {
            System.err.println("Missing value for " + flag);
            System.exit(2);
        }
        return args[index];
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

    private static VectorSchemaRoot makeRandomBatch(BufferAllocator alloc, long rows, long seed, String valName) {
        Field key = new Field("key", FieldType.nullable(new ArrowType.Int(64, true)), null);
        Field val = new Field(valName, FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(Arrays.asList(key, val));
        VectorSchemaRoot root = VectorSchemaRoot.create(schema, alloc);
        BigIntVector keyVec = (BigIntVector) root.getVector("key");
        IntVector valVec = (IntVector) root.getVector(valName);
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
        boolean twoSided = false;
        String thriftPathArg = null;
        String thriftStructArg = null;
        String payloadLeftArg = null;
        String payloadRightArg = null;
        String keyFieldArg = null;

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];
            switch (arg) {
                case "two":
                case "--two":
                    twoSided = true;
                    break;
                case "one":
                case "--one":
                    twoSided = false;
                    break;
                case "--thrift-path":
                    thriftPathArg = requireArg(args, ++i, "--thrift-path");
                    break;
                case "--thrift-struct":
                    thriftStructArg = requireArg(args, ++i, "--thrift-struct");
                    break;
                case "--payload-left":
                    payloadLeftArg = requireArg(args, ++i, "--payload-left");
                    break;
                case "--payload-right":
                    payloadRightArg = requireArg(args, ++i, "--payload-right");
                    break;
                case "--key-field":
                    keyFieldArg = requireArg(args, ++i, "--key-field");
                    break;
                default:
                    System.err.println("Unknown argument: " + arg);
                    System.exit(2);
            }
        }

        long rowsPerRank = getEnvLong("SHUFFLE_LOAD_ROWS", 200_000);
        long iters = getEnvLong("SHUFFLE_LOAD_ITERS", 3);
        String outCsv = getEnv("SHUFFLE_LOAD_OUT");
        long seedBase = getEnvLong("SHUFFLE_TEST_SEED", System.currentTimeMillis());
        String thriftPath = getEnv("COGROUP_THRIFT_FILE");
        String thriftStruct = getEnv("COGROUP_THRIFT_STRUCT");
        String payloadLeftPath = getEnv("COGROUP_THRIFT_PAYLOAD_LEFT");
        if (payloadLeftPath.isEmpty()) payloadLeftPath = getEnv("COGROUP_THRIFT_PAYLOAD");
        String payloadRightPath = getEnv("COGROUP_THRIFT_PAYLOAD_RIGHT");
        String keyField = getEnv("COGROUP_KEY_FIELD");
        if (keyField.isEmpty()) keyField = "key";
        boolean thriftEnabled = !payloadLeftPath.isEmpty() && !thriftPath.isEmpty() && !thriftStruct.isEmpty();

        if (thriftPathArg == null || thriftPathArg.isEmpty()) {
            thriftPathArg = getEnv("SHUFFLE_THRIFT_PATH", "");
        }
        if (thriftStructArg == null || thriftStructArg.isEmpty()) {
            thriftStructArg = getEnv("SHUFFLE_THRIFT_STRUCT", "");
        }
        if (payloadLeftArg == null || payloadLeftArg.isEmpty()) {
            payloadLeftArg = getEnv("SHUFFLE_THRIFT_PAYLOAD_LEFT", "");
        }
        if (payloadRightArg == null || payloadRightArg.isEmpty()) {
            payloadRightArg = getEnv("SHUFFLE_THRIFT_PAYLOAD_RIGHT", "");
        }
        if (keyFieldArg == null || keyFieldArg.isEmpty()) {
            keyFieldArg = getEnv("SHUFFLE_THRIFT_KEY_FIELD", getEnv("SHUFFLE_LOAD_KEY", "key"));
        }

        final boolean useThrift = !thriftPathArg.isEmpty() && !thriftStructArg.isEmpty() && !payloadLeftArg.isEmpty();

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

        try (RootAllocator alloc = new RootAllocator()) {
            CogroupInputFactory.ThriftInputs thriftInputs = null;
            VectorSchemaRoot left = null;
            VectorSchemaRoot right = null;
            String keyFieldResolved = keyFieldArg == null ? "key" : keyFieldArg;
            long rowsPerRankResolved = rowsPerRank;
            try {
                if (useThrift) {
                    thriftInputs = CogroupInputFactory.fromThrift(
                            alloc,
                            thriftPathArg,
                            thriftStructArg,
                            payloadLeftArg,
                            payloadRightArg.isEmpty() ? null : payloadRightArg,
                            keyFieldResolved,
                            rank,
                            world);
                    left = thriftInputs.left;
                    right = thriftInputs.right;
                    keyFieldResolved = thriftInputs.keyField;
                    rowsPerRankResolved = thriftInputs.rowsPerRank;

                    if (rank == 0) {
                        if (thriftInputs.leftDecodeSeconds > 0.0) {
                            double rowsPerSec = thriftInputs.rowsPerRank / thriftInputs.leftDecodeSeconds;
                            double mbPerSec = (thriftInputs.leftBytes / (1024.0 * 1024.0)) / thriftInputs.leftDecodeSeconds;
                            System.out.printf(
                                    "[JniCogroupLoad] thrift_decode_left rows=%d time_s=%.6f rows_per_sec=%.1f mb_per_sec=%.1f%n",
                                    left.getRowCount(), thriftInputs.leftDecodeSeconds, rowsPerSec, mbPerSec);
                        }
                        if (thriftInputs.rightDecodeSeconds > 0.0) {
                            double rowsPerSec = right.getRowCount() / thriftInputs.rightDecodeSeconds;
                            double mbPerSec = (thriftInputs.rightBytes / (1024.0 * 1024.0)) / thriftInputs.rightDecodeSeconds;
                            System.out.printf(
                                    "[JniCogroupLoad] thrift_decode_right rows=%d time_s=%.6f rows_per_sec=%.1f mb_per_sec=%.1f%n",
                                    right.getRowCount(), thriftInputs.rightDecodeSeconds, rowsPerSec, mbPerSec);
                        }
                    }
                } else {
                    left = makeRandomBatch(alloc, rowsPerRank, seedBase + rank, "la");
                    right = makeRandomBatch(alloc, rowsPerRank, seedBase + rank + 17, "rb");
                }

                long rowsLocal = (long) left.getRowCount() + (right != null ? right.getRowCount() : 0);
                long rowsGlobal = rowsLocal * Math.max(1, world);

                if (rank == 0) {
                    System.out.println("[JniCogroupLoad] world=" + world +
                            " rows_per_rank=" + rowsPerRankResolved +
                            " iters=" + iters +
                            " mode=" + mode +
                            (useThrift ? (" thrift=" + thriftStructArg + " key_field=" + keyFieldResolved) : ""));
                    if (useThrift) {
                        System.out.println("[JniCogroupLoad] left_payload=" + payloadLeftArg +
                                (payloadRightArg.isEmpty() ? "" : (" right_payload=" + payloadRightArg)));
                    }
                }

                VectorSchemaRoot[] warm = NativeProcessors.cogroup(alloc, left, right, keyFieldResolved, !twoSided, rank, world);
                if (warm != null) {
                    for (VectorSchemaRoot v : warm) if (v != null) v.close();
                }

                double best = Double.MAX_VALUE, total = 0.0;
                for (int i = 0; i < iters; i++) {
                    long t0 = System.nanoTime();
                    VectorSchemaRoot[] out = NativeProcessors.cogroup(alloc, left, right, keyFieldResolved, !twoSided, rank, world);
                    long t1 = System.nanoTime();
                    if (out != null) {
                        for (VectorSchemaRoot v : out) if (v != null) v.close();
                    }
                    double dt = (t1 - t0) / 1e9;
                    best = Math.min(best, dt);
                    total += dt;

                    double rps = rowsGlobal / dt;
                    if (rank == 0) {
                        System.out.println("[JniCogroupLoad] iter=" + i +
                                " rows=" + rowsGlobal +
                                " time_s=" + dt +
                                " rows_per_sec=" + rps);
                        if (csvEnabled) {
                            long epoch = Instant.now().getEpochSecond();
                            csv.write(epoch + "," + epoch + "," + mode + "," + world + "," + rowsPerRankResolved + "," + i + "," + rowsGlobal + "," + dt + "," + rps + "\n");
                            csv.flush();
                        }
                    }
                }
                if (rank == 0) {
                    double avg = total / (double) iters;
                    System.out.println("[JniCogroupLoad] best_s=" + best + " avg_s=" + avg);
                }
            } finally {
                if (thriftInputs != null) {
                    thriftInputs.close();
                } else {
                    if (left != null) left.close();
                    if (right != null) right.close();
                }
            }
        }

        if (csvEnabled) {
            csv.close();
        }
    }
}
