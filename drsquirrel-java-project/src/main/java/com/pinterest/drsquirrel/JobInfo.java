package com.pinterest.drsquirrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.drsquirrel.parsers.DrSquirrelUtils;
import com.pinterest.drsquirrel.schema.FlinkJobInfo;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.JobState;
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

/**
 * user expect to override com.pinterest.drsquirrel.Bridge and implement thier own process
 * function
 */
public class JobInfo {
    protected static final BufferAllocator allocator = new RootAllocator();
    protected static final Logger LOG = LoggerFactory.getLogger(JobInfo.class);
    private static final ObjectMapper mapper = new ObjectMapper();
    static Map<String, JobState> jobStates = new HashMap<>();

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

    protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
        VarCharVector jobid_in = (VarCharVector) input.getVector("jobid");
        VarCharVector type_in = (VarCharVector) input.getVector("type");
        VarCharVector json_in = (VarCharVector) input.getVector("json");
        /**
         * apply metric to job state
         */
        for (int i = 0; i < input.getRowCount(); i++) {
            String jobId = Text.decode(jobid_in.get(i));
            String JobType = Text.decode(type_in.get(i));
            String jsonstr = Text.decode(json_in.get(i));
            if(jobStates.get(jobId) == null) {
                jobStates.put(jobId, new JobState());
            }
            JobState state = jobStates.get(jobId);

            if(JobType.equals("metric")) {
                FlinkMetric flinkMetric = mapper.readValue(jsonstr, FlinkMetric.class);
                state.update(flinkMetric);
            }
        }

        VarCharVector jobinfo_out = new VarCharVector("jobinfo", allocator);

        jobinfo_out.allocateNew();
        int count = 0;
        /**
         * each application only has one state per batch
         */
        for (int i = 0; i < input.getRowCount(); i++) {
            String jobId = Text.decode(jobid_in.get(i));
            String JobType = Text.decode(type_in.get(i));

            JobState state = jobStates.get(jobId);

            if(JobType.equals("state")) {
                String jsonstr = Text.decode(json_in.get(i));
                State s = mapper.readValue(jsonstr, State.class);
                FlinkJobInfo jobInfo = s.toJobInfo(state);
                // serialize job info
                String jobInfoStr = DrSquirrelUtils.serializeJobInfo(jobInfo);
                System.out.print(jobInfoStr);
                jobinfo_out.setSafe(count, jobInfoStr.getBytes(StandardCharsets.UTF_8));
                count++;
            }
        }

        jobinfo_out.setSafe(count, new Text());
        count++;
        jobinfo_out.setValueCount(count);
        List<FieldVector> vectors = Arrays.asList(jobinfo_out);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
        vectorSchemaRoot.setRowCount(count);
        return vectorSchemaRoot;
    }
}
