import com.pinterest.drsquirrel.parsers.DrSquirrelUtils;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.FlinkMetricType;
import com.pinterest.drsquirrel.schema.RawLog;
import com.pinterest.drsquirrel.schema.Signal;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public class CleanupWrapper extends BaseWrapper {

    private boolean filterJobApplication(Signal signal) {
        return signal.getJobId() != null && signal.getApplicationId() != null
                && signal.getApplicationId() != "application_unknown";
    }

    @Override
    public VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
        VarCharVector jobId = new VarCharVector("jobid", allocator);
        VarCharVector appId = new VarCharVector("appid", allocator);
        VarCharVector type = new VarCharVector("type", allocator);
        VarCharVector json = new VarCharVector("json", allocator);
        jobId.allocateNew();
        appId.allocateNew();
        type.allocateNew();
        json.allocateNew();
        int count = 0;

        VarCharVector input_type = (VarCharVector) input.getVector("topic");
        VarCharVector input_payload = (VarCharVector) input.getVector("payload");

        for (int i = 0; i < input.getRowCount(); i++) {
            String topic = Text.decode(input_type.get(i));
            String body = Text.decode(input_payload.get(i));
            if (topic.equals("metric")) {
                FlinkMetric flinkMetric = DrSquirrelUtils.constructFlinkMetric(body);
                if (filterJobApplication(flinkMetric) && (flinkMetric.getType() == FlinkMetricType.JOBMANAGER_JOB
                        || flinkMetric.getType() == FlinkMetricType.JOBMANAGER
                        || flinkMetric.getType() == FlinkMetricType.JOBMANAGER_JOB_WITH_APPID
                        || flinkMetric.getType() == FlinkMetricType.JOBMANAGER_WITH_APPID)) {
                    try {
                        jobId.setSafe(count, flinkMetric.getJobId().getBytes(StandardCharsets.UTF_8));
                        appId.setSafe(count, flinkMetric.getApplicationId().getBytes(StandardCharsets.UTF_8));
                        type.setSafe(count, "metric".getBytes(StandardCharsets.UTF_8));
                        // keep payload size small
                        json.setSafe(count, mapper.writeValueAsString(flinkMetric).getBytes(StandardCharsets.UTF_8));
                        count++;
                    } catch (Exception e) {
                        LOG.warn("fail to parse", e);
                    }
                }
            }
            if (topic.equals("log")) {
                RawLog rawLog = DrSquirrelUtils.constructRawLog(body, "log");
                if (rawLog.getLevel().equals("ERROR") && filterJobApplication(rawLog)) {
                    try {
                        jobId.setSafe(count, rawLog.getApplicationId().getBytes(StandardCharsets.UTF_8));
                        appId.setSafe(count, rawLog.getApplicationId().getBytes(StandardCharsets.UTF_8));
                        type.setSafe(count, "log".getBytes(StandardCharsets.UTF_8));
                        json.setSafe(count, mapper.writeValueAsString(rawLog).getBytes(StandardCharsets.UTF_8));
                        count++;
                    } catch (Exception e) {
                        LOG.warn("fail to parse", e);
                    }
                }
            }
        }

        jobId.setValueCount(count);
        appId.setValueCount(count);
        type.setValueCount(count);
        json.setValueCount(count);
        List<FieldVector> vectors = Arrays.asList(jobId, appId, type, json);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
        vectorSchemaRoot.setRowCount(count);
        return vectorSchemaRoot;
    }

}
