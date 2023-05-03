import com.pinterest.drsquirrel.parsers.DrSquirrelUtils;
import com.pinterest.drsquirrel.schema.FlinkJobInfo;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.JobState;
import com.pinterest.drsquirrel.schema.State;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class JobInfoWrapper extends BaseWrapper {
    private final Map<String, JobState> jobStates;

    public JobInfoWrapper() {
        super();
        jobStates = new HashMap<>();
    }

    @Override
    public VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
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
            if (jobStates.get(jobId) == null) {
                jobStates.put(jobId, new JobState());
            }
            JobState state = jobStates.get(jobId);

            if (JobType.equals("metric")) {
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

            if (JobType.equals("state")) {
                String jsonstr = Text.decode(json_in.get(i));
                State s = mapper.readValue(jsonstr, State.class);
                FlinkJobInfo jobInfo = s.toJobInfo(state);
                // serialize job info
                String jobInfoStr = DrSquirrelUtils.serializeJobInfo(jobInfo);
                jobinfo_out.setSafe(count, jobInfoStr.getBytes(StandardCharsets.UTF_8));
                count++;
            }
        }
        jobinfo_out.setValueCount(count);
        List<FieldVector> vectors = Collections.singletonList(jobinfo_out);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
        vectorSchemaRoot.setRowCount(count);
        return vectorSchemaRoot;
    }
}
