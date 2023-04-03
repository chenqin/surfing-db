package com.pinterest.drsquirrel;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.pinterest.drsquirrel.parsers.DrSquirrelUtils;
import com.pinterest.drsquirrel.schema.FlinkMetric;
import com.pinterest.drsquirrel.schema.RawLog;
import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.util.Text;
import org.apache.commons.lang3.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** my bridge example */
public class Cleanup {
  protected static final BufferAllocator allocator = new RootAllocator();
  protected static final Logger LOG = LoggerFactory.getLogger(Cleanup.class);
  protected static long total = 0;
/**
   * Create a {@link VectorSchemaRoot} and export it via the C Data Interface
   *
   * @param schemaAddress Schema memory address to wrap
   * @param arrayAddress Array memory address to wrap
   */
  public static void _internal_invoke(long schemaIn, long arrayIn, long schemaOut, long arrayOut) {
    try (ArrowArray array_in = ArrowArray.wrap(arrayIn);
        ArrowSchema schema_in = ArrowSchema.wrap(schemaIn);
        ArrowArray array_out = ArrowArray.wrap(arrayOut);
        ArrowSchema schema_out = ArrowSchema.wrap(schemaOut)) {
      VectorSchemaRoot input = Data.importVectorSchemaRoot(allocator, array_in, schema_in, null);
      try {
        Data.exportVectorSchemaRoot(allocator, process(input), null, array_out, schema_out);
      } catch (Exception e) {
      } finally {
        input.clear();
      }
    }
  }
  private static ObjectMapper mapper = new ObjectMapper();
  
  protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
    total += input.getRowCount();

    VarCharVector jobId = new VarCharVector("jobid", allocator);
    VarCharVector type = new VarCharVector("type", allocator);
    VarCharVector json = new VarCharVector("json", allocator);
    jobId.allocateNew();
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

        if (flinkMetric.getJobId() != null) {
          try {
            jobId.setSafe(count, flinkMetric.getJobId().getBytes(StandardCharsets.UTF_8));
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
        try {
          jobId.setSafe(count, rawLog.getApplicationId().getBytes(StandardCharsets.UTF_8));
          type.setSafe(count, "log".getBytes(StandardCharsets.UTF_8));
          json.setSafe(count, mapper.writeValueAsString(rawLog).getBytes(StandardCharsets.UTF_8));
          count++;
        } catch (Exception e) {
          LOG.warn("fail to parse", e);
        }
      }
    }
    /**
     * insert a placeholder for now
     */
    jobId.setSafe(count, new Text(""));
    type.setSafe(count, new Text());
    json.setSafe(count, new Text());
    count++;

    jobId.setValueCount(count);
    type.setValueCount(count);
    json.setValueCount(count);
    List<FieldVector> vectors = Arrays.asList(jobId, type, json);
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
    vectorSchemaRoot.setRowCount(count);
    return vectorSchemaRoot;
  }
}
