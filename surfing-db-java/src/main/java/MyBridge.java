import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;

import com.pinterest.drsquirrel.parsers.DrSquirrelUtils;
import com.pinterest.drsquirrel.parsers.FlinkMetricParser;
import com.pinterest.drsquirrel.schema.FlinkJobInfo;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.FlinkMetricType;
import com.pinterest.drsquirrel.schema.RawLog;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.BitVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.util.Text;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * my bridge example
 */
public class MyBridge extends Bridge {
    /**
     * user defined function entry with zero copy
     * 
     * @param arrow memory pointer passing from c++
     * @return arrow memory pointer passing back to c++
     */
    protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception{
        total += input.getRowCount();
       
        VarCharVector jobId = new VarCharVector("jobid", allocator);
        jobId.allocateNew();
        int count = 0;
        jobId.setSafe(count++, new Text());
       
        VarCharVector input_type = (VarCharVector) input.getVector("topic");
        VarCharVector input_payload = (VarCharVector) input.getVector("payload");
        
        for(int i = 0 ; i < input.getRowCount(); i++) {
            String topic = Text.decode(input_type.get(i));
            String body = Text.decode(input_payload.get(i));
            if(topic.equals("metric")) {
                FlinkMetric flinkMetric = DrSquirrelUtils.constructFlinkMetric(body);
                
                if(flinkMetric.getJobId() != null
                    && (flinkMetric.getType() == FlinkMetricType.JOBMANAGER_JOB
                    || flinkMetric.getType() == FlinkMetricType.JOBMANAGER
                    || flinkMetric.getType() == FlinkMetricType.JOBMANAGER_JOB_WITH_APPID
                    || flinkMetric.getType() == FlinkMetricType.JOBMANAGER_WITH_APPID)) {
                    
                    //String json_str = mapper.writeValueAsString(flinkMetric);
                    jobId.setSafe(count, flinkMetric.getJobId().getBytes(StandardCharsets.UTF_8));
                    //json.setSafe(count, json_str.getBytes(StandardCharsets.UTF_8));
                    count++;
                }
                
            } else if (topic.equals("log")) {
                RawLog rawLog = DrSquirrelUtils.constructRawLog(body, "log");
                //String json_str = mapper.writeValueAsString(rawLog);
                //System.out.println(json_str);
                
                if(rawLog.getFile().contains("ExecutionGraph")) {
                    //String json_str = mapper.writeValueAsString(rawLog);
                    jobId.setSafe(count, rawLog.getApplicationId().getBytes(StandardCharsets.UTF_8));
                    //json.setSafe(count, json_str.getBytes(StandardCharsets.UTF_8));
                    count++;
                }
            } else {
                throw new RuntimeException("topic is not supported");
            }
        }

        jobId.setValueCount(count);
        List<FieldVector> vectors = Arrays.asList(jobId);
        VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
        vectorSchemaRoot.setRowCount(count);
        return vectorSchemaRoot;
    }
}