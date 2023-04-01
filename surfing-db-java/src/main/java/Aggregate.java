import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.RawLog;
import com.pinterest.drsquirrel.schema.State;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Aggregate extends Bridge{
    static Map<String, State> states = new HashMap<>();
    private static ObjectMapper mapper = new ObjectMapper();
    protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception{
        VarCharVector jobid = (VarCharVector) input.getVector("jobid");
        VarCharVector type = (VarCharVector) input.getVector("type");
        VarCharVector json = (VarCharVector) input.getVector("json");
        for (int i = 0; i < input.getRowCount(); i++) {
          String JobID = Text.decode(jobid.get(i));
          String JobType = Text.decode(type.get(i));
          String JSON = Text.decode(json.get(i));
          if(states.get(JobID.toLowerCase()) == null) {
              State s = new State();
              s.maxNumOfLatestExceptions = 20;
              states.put(JobID.toLowerCase(), s);
          }
          if(JobType.equalsIgnoreCase("metric")) {
              FlinkMetric flinkMetric = mapper.readValue(JSON, FlinkMetric.class);
              states.get(JobID.toLowerCase()).update(flinkMetric);
          }
          if(JobType.equalsIgnoreCase("log")) {
              RawLog rawLog = mapper.readValue(JSON, RawLog.class);
              states.get(JobID.toLowerCase()).update(rawLog);
          }
        }

        VarCharVector app = new VarCharVector("jobid", allocator);
        VarCharVector snapshot = new VarCharVector("json", allocator);
        app.allocateNew();
        snapshot.allocateNew();
        int count = 0;

        app.setSafe(count, new Text());
        snapshot.setSafe(count, new Text());
        count++;

        for(Map.Entry<String, State> state : states.entrySet()) {
            app.setSafe(count, state.getKey().getBytes(StandardCharsets.UTF_8));
            String json_str = mapper.writeValueAsString(state.getValue());
            snapshot.setSafe(count, json_str.getBytes(StandardCharsets.UTF_8));
            count++;
        }

        app.setValueCount(count);
        snapshot.setValueCount(count);
        List<FieldVector> vectors = Arrays.asList(app, snapshot);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
        vectorSchemaRoot.setRowCount(count);
        return vectorSchemaRoot;
    }
}
