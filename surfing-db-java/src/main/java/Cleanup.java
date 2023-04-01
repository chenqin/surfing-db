import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.drsquirrel.parsers.DrSquirrelUtils;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.FlinkMetricType;
import com.pinterest.drsquirrel.schema.RawLog;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** my bridge example */
public class Cleanup extends Bridge {

  private static ObjectMapper mapper = new ObjectMapper();
  /**
   * user defined function entry with zero copy
   *
   * @param arrow memory pointer passing from c++
   * @return arrow memory pointer passing back to c++
   */
  protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
    total += input.getRowCount();

    VarCharVector jobId = new VarCharVector("jobid", allocator);
    VarCharVector type = new VarCharVector("type", allocator);
    VarCharVector json = new VarCharVector("json", allocator);
    jobId.allocateNew(input.getRowCount());
    type.allocateNew(input.getRowCount());
    json.allocateNew(input.getRowCount());
    int count = 0;

    jobId.setSafe(count, new Text());
    type.setSafe(count, new Text());
    json.setSafe(count, new Text());
    count++;

    VarCharVector input_type = (VarCharVector) input.getVector("topic");
    VarCharVector input_payload = (VarCharVector) input.getVector("payload");

    for (int i = 0; i < input.getRowCount(); i++) {
      String topic = Text.decode(input_type.get(i));
      String body = Text.decode(input_payload.get(i));
      if (topic.equals("metric")) {
        FlinkMetric flinkMetric = DrSquirrelUtils.constructFlinkMetric(body);

        if (flinkMetric.getJobId() != null
            && (flinkMetric.getType() == FlinkMetricType.JOBMANAGER_JOB
                || flinkMetric.getType() == FlinkMetricType.JOBMANAGER
                || flinkMetric.getType() == FlinkMetricType.JOBMANAGER_JOB_WITH_APPID
                || flinkMetric.getType() == FlinkMetricType.JOBMANAGER_WITH_APPID)) {
          try {
            String json_str = mapper.writeValueAsString(flinkMetric);
            jobId.setSafe(count, flinkMetric.getJobId().getBytes(StandardCharsets.UTF_8));
            type.setSafe(count, "metric".getBytes(StandardCharsets.UTF_8));
            json.setSafe(count, json_str.getBytes(StandardCharsets.UTF_8));
            count++;
          } catch (Exception e) {
            LOG.warn("fail to parse", e);
          }
        }

      } else if (topic.equals("log")) {
        RawLog rawLog = DrSquirrelUtils.constructRawLog(body, "log");
        if (rawLog.getFile().contains("ExecutionGraph")) {
          try {
            String json_str = mapper.writeValueAsString(rawLog);
            jobId.setSafe(count, rawLog.getApplicationId().getBytes(StandardCharsets.UTF_8));
            type.setSafe(count, "log".getBytes(StandardCharsets.UTF_8));
            json.setSafe(count, json_str.getBytes(StandardCharsets.UTF_8));
            count++;
          } catch (Exception e) {
            LOG.warn("fail to parse", e);
          }
        }
      } else {
        throw new RuntimeException("topic is not supported");
      }
    }

    jobId.setValueCount(count);
    type.setValueCount(count);
    json.setValueCount(count);
    List<FieldVector> vectors = Arrays.asList(jobId, type, json);
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
    vectorSchemaRoot.setRowCount(count);
    return vectorSchemaRoot;
  }
}
