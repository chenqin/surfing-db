package com.pinterest.drsquirrel.jni;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.VectorUnloader;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowFileWriter;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.Base64;

import com.pinterest.drsquirrel.jni.NativeThriftDecoder; // from surfingthriftjni module

/**
 * MPI worker loop that leases tasks from an MCP server and executes them sequentially across the MPI world.
 *
 * Each task JSON should look like:
 * {
 *   "taskId": "uuid-...",
 *   "mode": "shuffle" | "cogroup",
 *   "oneSided": true,
 *   "keyField": "key",
 *   "leftS3": ["s3://bucket/prefix1"],
 *   "rightS3": ["s3://bucket/prefix2"],            // required for cogroup
 *   "thrift": { "dir": "s3://bucket/thrift/", "struct": "MyStruct", "file": "schema.thrift" },
 *   "outputS3": "s3://bucket/output/prefix"
 * }
 */
public final class McpWorkerRunner {
    private static final ObjectMapper M = new ObjectMapper();

    private static String getenv(String k, String d) { String v = System.getenv(k); return v != null ? v : d; }

    private static String readAll(InputStream is) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] buf = new byte[8192];
        int r;
        while ((r = is.read(buf)) != -1) bos.write(buf, 0, r);
        return new String(bos.toByteArray(), StandardCharsets.UTF_8);
    }

    private static String httpGet(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("GET");
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(30000);
        int code = conn.getResponseCode();
        InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
        String s = readAll(is);
        conn.disconnect();
        if (code >= 200 && code < 300) return s; else throw new IOException("HTTP " + code + ": " + s);
    }

    private static String httpPostJson(String url, String body) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestMethod("POST");
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        try (OutputStream os = conn.getOutputStream()) { os.write(body.getBytes(StandardCharsets.UTF_8)); }
        int code = conn.getResponseCode();
        InputStream is = (code >= 200 && code < 300) ? conn.getInputStream() : conn.getErrorStream();
        String s = readAll(is);
        conn.disconnect();
        if (code >= 200 && code < 300) return s; else throw new IOException("HTTP " + code + ": " + s);
    }

    private static int sh(String... cmd) throws IOException, InterruptedException {
        Process p = new ProcessBuilder(cmd).inheritIO().start();
        return p.waitFor();
    }

    private static void awsSync(String s3, Path local) throws IOException, InterruptedException {
        Files.createDirectories(local);
        int rc = sh("bash", "-lc", "aws s3 cp --recursive '" + s3 + "' '" + local.toString() + "'");
        if (rc != 0) throw new IOException("aws s3 cp failed for " + s3);
    }

    private static Path pickThrift(Path thriftDir, String structName, String explicitFile) throws IOException {
        if (explicitFile != null && !explicitFile.isEmpty()) {
            Path p = thriftDir.resolve(explicitFile);
            if (Files.exists(p)) return p;
        }
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(thriftDir, "*.thrift")) {
            for (Path p : ds) {
                String txt = new String(Files.readAllBytes(p), StandardCharsets.UTF_8);
                if (txt.contains("struct " + structName)) return p;
            }
        }
        throw new FileNotFoundException("No thrift file containing struct " + structName + " in " + thriftDir);
    }

    private static List<byte[]> readPayloadsFromDir(Path dir) throws IOException {
        List<byte[]> out = new ArrayList<>();
        try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
            for (Path p : ds) {
                if (!Files.isRegularFile(p)) continue;
                try (BufferedReader br = Files.newBufferedReader(p, StandardCharsets.UTF_8)) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        line = line.trim();
                        if (line.isEmpty() || line.startsWith("#")) continue;
                        out.add(Base64.getDecoder().decode(line));
                    }
                }
            }
        }
        return out;
    }

    private static void writeArrowFile(Path path, VectorSchemaRoot root) throws IOException {
        try (FileOutputStream fos = new FileOutputStream(path.toFile());
             org.apache.arrow.vector.ipc.ArrowFileWriter w = new ArrowFileWriter(root, null, fos.getChannel())) {
            w.start();
            w.writeBatch();
            w.end();
        }
    }

    public static void main(String[] args) throws Exception {
        String server = getenv("MCP_SERVER", args.length > 0 ? args[0] : "http://localhost:8080");
        long pollMs = Long.parseLong(getenv("MCP_POLL_MS", "2000"));
        boolean oneSidedDefault = Boolean.parseBoolean(getenv("MCP_ONE_SIDED", "true"));

        int rank = NativeProcessors.getMpiRank();
        int world = NativeProcessors.getMpiWorld();

        System.out.printf("[MCP] rank=%d world=%d server=%s\n", rank, world, server);
        BufferAllocator alloc = new RootAllocator();

        // Support single-task mode via env MCP_TASK_JSON or arg prefix json:
        String singleTaskJson = System.getenv("MCP_TASK_JSON");
        if (singleTaskJson == null && args.length > 0 && args[0] != null && args[0].startsWith("json:")) {
            singleTaskJson = new String(java.util.Base64.getDecoder().decode(args[0].substring(5)), java.nio.charset.StandardCharsets.UTF_8);
        }

        boolean singleMode = singleTaskJson != null && !singleTaskJson.isEmpty();

        while (true) {
            String leasedJson = null;
            if (singleMode) {
                leasedJson = singleTaskJson;
            } else if (rank == 0) {
                try {
                    leasedJson = httpGet(server + "/lease");
                    if (leasedJson != null && leasedJson.trim().equals("NONE")) leasedJson = null;
                } catch (Exception e) {
                    System.err.println("[MCP] lease error: " + e);
                    leasedJson = null;
                }
            }
            // Broadcast task JSON (empty means no task)
            String taskJson = NativeProcessors.bcast(leasedJson == null ? "" : leasedJson);
            if (taskJson == null || taskJson.isEmpty()) {
                Thread.sleep(pollMs);
                continue;
            }

            JsonNode task = M.readTree(taskJson);
            String taskId = task.path("taskId").asText(UUID.randomUUID().toString());
            String mode = task.path("mode").asText("shuffle");
            boolean oneSided = task.has("oneSided") ? task.path("oneSided").asBoolean() : oneSidedDefault;
            String keyField = task.path("keyField").asText("key");
            String outS3 = task.path("outputS3").asText();
            if (outS3 == null || outS3.isEmpty()) throw new IllegalArgumentException("outputS3 required");

            // Stage inputs
            Path workDir = Paths.get(System.getProperty("java.io.tmpdir"), "mcp_task_" + taskId + "_r" + rank);
            Files.createDirectories(workDir);

            JsonNode thrift = task.path("thrift");
            String thriftDirS3 = thrift.path("dir").asText();
            String structName = thrift.path("struct").asText();
            String thriftFileName = thrift.path("file").asText("");
            if (structName == null || structName.isEmpty()) throw new IllegalArgumentException("thrift.struct required");

            Path thriftDir = workDir.resolve("thrift");
            awsSync(thriftDirS3, thriftDir);
            Path thriftFile = pickThrift(thriftDir, structName, thriftFileName);

            VectorSchemaRoot left = null, right = null;
            if (mode.equalsIgnoreCase("shuffle")) {
                // In shuffle mode, use leftS3 set only
                List<String> leftS3 = new ArrayList<>();
                if (task.has("leftS3")) task.path("leftS3").forEach(n -> leftS3.add(n.asText()));
                if (leftS3.isEmpty() && task.has("inputS3")) task.path("inputS3").forEach(n -> leftS3.add(n.asText()));
                if (leftS3.isEmpty()) throw new IllegalArgumentException("leftS3/inputS3 required for shuffle");

                Path inDir = workDir.resolve("in_left"); Files.createDirectories(inDir);
                for (String s3 : leftS3) awsSync(s3, inDir);
                List<byte[]> payloads = readPayloadsFromDir(inDir);
                byte[][] arr = payloads.toArray(new byte[0][]);
                left = NativeThriftDecoder.convert(alloc, arr, thriftFile.toString(), structName);
            } else if (mode.equalsIgnoreCase("cogroup")) {
                List<String> leftS3 = new ArrayList<>();
                List<String> rightS3 = new ArrayList<>();
                if (task.has("leftS3")) task.path("leftS3").forEach(n -> leftS3.add(n.asText()));
                if (task.has("rightS3")) task.path("rightS3").forEach(n -> rightS3.add(n.asText()));
                if (leftS3.isEmpty() || rightS3.isEmpty()) throw new IllegalArgumentException("leftS3/rightS3 required for cogroup");

                Path inL = workDir.resolve("in_left"); Files.createDirectories(inL);
                Path inR = workDir.resolve("in_right"); Files.createDirectories(inR);
                for (String s3 : leftS3) awsSync(s3, inL);
                for (String s3 : rightS3) awsSync(s3, inR);
                List<byte[]> payL = readPayloadsFromDir(inL);
                List<byte[]> payR = readPayloadsFromDir(inR);
                left = NativeThriftDecoder.convert(alloc, payL.toArray(new byte[0][]), thriftFile.toString(), structName);
                right = NativeThriftDecoder.convert(alloc, payR.toArray(new byte[0][]), thriftFile.toString(), structName);
            } else {
                throw new IllegalArgumentException("Unknown mode: " + mode);
            }

            // Execute
            if (mode.equalsIgnoreCase("shuffle")) {
                VectorSchemaRoot out = NativeProcessors.shuffle(alloc, left, keyField, oneSided, rank, world);
                Path outPath = workDir.resolve("rank-" + rank + ".arrow");
                writeArrowFile(outPath, out);
                out.close(); left.close();
                // Upload
                sh("bash", "-lc", "aws s3 cp '" + outPath + "' '" + outS3 + "/rank-" + rank + ".arrow'");
            } else {
                VectorSchemaRoot[] outs = NativeProcessors.cogroup(alloc, left, right, keyField, oneSided, rank, world);
                Path outL = workDir.resolve("rank-" + rank + "-left.arrow");
                Path outR = workDir.resolve("rank-" + rank + "-right.arrow");
                writeArrowFile(outL, outs[0]);
                writeArrowFile(outR, outs[1]);
                outs[0].close(); outs[1].close(); left.close(); right.close();
                sh("bash", "-lc", "aws s3 cp '" + outL + "' '" + outS3 + "/rank-" + rank + "-left.arrow'");
                sh("bash", "-lc", "aws s3 cp '" + outR + "' '" + outS3 + "/rank-" + rank + "-right.arrow'");
            }

            NativeProcessors.mpiBarrier();
            if (!singleMode) {
                if (rank == 0) {
                    // Mark complete
                    String completeBody = M.createObjectNode()
                            .put("taskId", taskId)
                            .put("status", "done")
                            .toString();
                    try { httpPostJson(server + "/complete", completeBody); } catch (Exception e) {
                        System.err.println("[MCP] complete error: " + e);
                    }
                }
                NativeProcessors.mpiBarrier();
            }
            // Cleanup local work dir
            try { Files.walk(workDir).sorted(Comparator.reverseOrder()).forEach(p -> { try { Files.deleteIfExists(p); } catch (IOException ignore) {} }); } catch (Exception ignore) {}

            if (singleMode) break;
        }
    }
}
