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
import java.util.*;

public class Aggregate {
    protected static final BufferAllocator allocator = new RootAllocator();
    protected static final Logger LOG = LoggerFactory.getLogger(Aggregate.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    protected static long total = 0;
    static Map<String, State> states = new HashMap<>();

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
                System.out.println(e.getMessage());
            } finally {
                if (input != null) input.close();
                if (output != null) output.close();
            }
        }
    }

    /**
     * generate app_state from appId, pass through signals with non empty jobid
     *
     * @param input
     * @return
     * @throws Exception
     */
    protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
        /**
         * input vectors
         */
        VarCharVector appid_in = (VarCharVector) input.getVector("appid");
        VarCharVector jobid_in = (VarCharVector) input.getVector("jobid");
        VarCharVector type_in = (VarCharVector) input.getVector("type");
        VarCharVector json_in = (VarCharVector) input.getVector("json");

        /**
         * output vectors
         */
        VarCharVector jobid_out = new VarCharVector("jobid", allocator);
        VarCharVector type_out = new VarCharVector("type", allocator);
        VarCharVector json_out = new VarCharVector("json", allocator);

        jobid_out.allocateNew();
        type_out.allocateNew();
        json_out.allocateNew();
        int count = 0;


        for (int i = 0; i < input.getRowCount(); i++) {
            String appID = Text.decode(appid_in.get(i));
            String jobId = Text.decode(jobid_in.get(i));
            String JobType = Text.decode(type_in.get(i));
            String jsonstr = Text.decode(json_in.get(i));
            /**
             * build per app_id state
             */
            if (states.get(appID.toLowerCase()) == null) {
                State s = new State();
                s.maxNumOfLatestExceptions = 20;
                states.put(appID.toLowerCase(), s);
            }

            /**
             * assign jobId of same applicationID and last update time
             */
            if (JobType.equalsIgnoreCase("metric")) {
                FlinkMetric flinkMetric = mapper.readValue(jsonstr, FlinkMetric.class);
                states.get(appID.toLowerCase()).update(flinkMetric);
            }
            if (JobType.equalsIgnoreCase("log")) {
                RawLog rawLog = mapper.readValue(jsonstr, RawLog.class);
                states.get(appID.toLowerCase()).update(rawLog);
            }

            /**
             * pass through metric with jobid if stored
             */
            if(jobId == null || jobId.length() < 1) {
                jobId = states.get(appID.toLowerCase()).getJobId();
            }

            if (jobId != null && JobType.equalsIgnoreCase("metric")) {
                jobid_out.setSafe(count, jobId.getBytes(StandardCharsets.UTF_8));
                type_out.setSafe(count, JobType.getBytes(StandardCharsets.UTF_8));
                json_out.setSafe(count, jsonstr.getBytes(StandardCharsets.UTF_8));
                count++;
            }

        }

        Iterator<Map.Entry<String, State>> it = states.entrySet().iterator();
        while(it.hasNext()) {
            Map.Entry<String, State> state = it.next();

            if (!inTTL(state.getValue())) {
                it.remove();
                continue;
            }

            jobid_out.setSafe(count, state.getValue().getJobId().getBytes(StandardCharsets.UTF_8));
            type_out.setSafe(count, "state".getBytes(StandardCharsets.UTF_8));
            String json_str = mapper.writeValueAsString(state.getValue());
            json_out.setSafe(count, json_str.getBytes(StandardCharsets.UTF_8));
            count++;
        }

        /**
         * insert a placeholder for now
         */
        jobid_out.setSafe(count, new Text(""));
        type_out.setSafe(count, new Text());
        json_out.setSafe(count, new Text());
        count++;

        jobid_out.setValueCount(count);
        json_out.setValueCount(count);
        List<FieldVector> vectors = Arrays.asList(jobid_out, type_out, json_out);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
        vectorSchemaRoot.setRowCount(count);
        return vectorSchemaRoot;
    }

    public static long TTL = 24 * 60 * 60 * 1000;

    static boolean inTTL(State s) {
        return s.getLastUpdatedTimestamp() > System.currentTimeMillis() - TTL;
    }
}
