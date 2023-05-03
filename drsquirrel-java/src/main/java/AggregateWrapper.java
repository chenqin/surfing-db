import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.RawLog;
import com.pinterest.drsquirrel.schema.State;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class AggregateWrapper extends BaseWrapper {
    public static final long TTL = 24 * 60 * 60 * 1000;
    final Map<String, State> states;

    public AggregateWrapper() {
        super();
        states = new HashMap<>();
    }

    @Override
    public VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
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
            if (jobId == null || jobId.length() < 1) {
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
        while (it.hasNext()) {
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

        jobid_out.setValueCount(count);
        json_out.setValueCount(count);
        List<FieldVector> vectors = Arrays.asList(jobid_out, type_out, json_out);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
        vectorSchemaRoot.setRowCount(count);
        return vectorSchemaRoot;
    }

    boolean inTTL(State s) {
        return s.getLastUpdatedTimestamp() > System.currentTimeMillis() - TTL;
    }
}
