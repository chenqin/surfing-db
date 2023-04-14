package com.pinterest.drsquirrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.RawLog;
import com.pinterest.drsquirrel.schema.State;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregate {
    protected static final BufferAllocator allocator = new RootAllocator();
    protected static final Logger LOG = LoggerFactory.getLogger(Aggregate.class);
    protected static long total = 0;
    static Map<String, State> states = new HashMap<>();
    private static final ObjectMapper mapper = new ObjectMapper();

    /**
     * Create a {@link VectorSchemaRoot} and export it via the C Data Interface
     *
     * @param schemaAddress Schema memory address to wrap
     * @param arrayAddress  Array memory address to wrap
     */
    public static void _internal_invoke(long schemaIn, long arrayIn, long schemaOut, long arrayOut) {
        try (ArrowArray array_in = ArrowArray.wrap(arrayIn);
             ArrowSchema schema_in = ArrowSchema.wrap(schemaIn);
             ArrowArray array_out = ArrowArray.wrap(arrayOut);
             ArrowSchema schema_out = ArrowSchema.wrap(schemaOut)) {
            VectorSchemaRoot input = null;
            VectorSchemaRoot output = null;
            try {
                input = Data.importVectorSchemaRoot(allocator, array_in, schema_in, null);
                output = process(input);
                Data.exportVectorSchemaRoot(allocator, output, null, array_out, schema_out);
            } catch (Exception e) {

            } finally {
                if (input != null) input.close();
                if (output != null) output.close();
            }
        }
    }

    protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
        VarCharVector appid = (VarCharVector) input.getVector("appid");
        VarCharVector type = (VarCharVector) input.getVector("type");
        VarCharVector binary = (VarCharVector) input.getVector("json");
        for (int i = 0; i < input.getRowCount(); i++) {
            String appID = Text.decode(appid.get(i));
            String JobType = Text.decode(type.get(i));
            String jsonstr = Text.decode(binary.get(i));
            if (states.get(appID.toLowerCase()) == null) {
                State s = new State();
                s.maxNumOfLatestExceptions = 20;
                states.put(appID.toLowerCase(), s);
            }
            if (JobType.equalsIgnoreCase("metric")) {
                FlinkMetric flinkMetric = mapper.readValue(jsonstr, FlinkMetric.class);
                states.get(appID.toLowerCase()).update(flinkMetric);
            }
            if (JobType.equalsIgnoreCase("log")) {
                RawLog rawLog = mapper.readValue(jsonstr, RawLog.class);
                states.get(appID.toLowerCase()).update(rawLog);
            }
        }

        VarCharVector jobId = new VarCharVector("jobid", allocator);
        VarCharVector snapshot = new VarCharVector("json", allocator);
        jobId.allocateNew();
        snapshot.allocateNew();
        int count = 0;

        for (Map.Entry<String, State> state : states.entrySet()) {
            if (!inTTL(state.getValue())) continue;
            jobId.setSafe(count, state.getValue().getJobId().getBytes(StandardCharsets.UTF_8));
            String json_str = mapper.writeValueAsString(state.getValue());
            snapshot.setSafe(count, json_str.getBytes(StandardCharsets.UTF_8));
            count++;
        }
        /**
         * insert a placeholder for now
         */
        jobId.setSafe(count, new Text(""));
        snapshot.setSafe(count, new Text());
        count++;

        jobId.setValueCount(count);
        snapshot.setValueCount(count);
        List<FieldVector> vectors = Arrays.asList(jobId, snapshot);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
        vectorSchemaRoot.setRowCount(count);
        return vectorSchemaRoot;
    }

    static boolean inTTL(State s) {
        return s.getLastUpdatedTimestamp() > System.currentTimeMillis() - 24 * 60 * 60 * 1000;
    }
}
