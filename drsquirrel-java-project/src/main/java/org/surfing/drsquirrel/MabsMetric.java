package org.surfing.drsquirrel;

import org.surfing.drsquirrel.mabsutils.FlattenInputMabsMetrics;
import org.surfing.drsquirrel.mabsutils.MabsBaseMetric;
import org.surfing.drsquirrel.mabsutils.MabsMetrics;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.flatbuffers.LongVector;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.impl.UnionMapReader;
import org.apache.arrow.vector.util.Text;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/**
 * user expect to override org.surfing.drsquirrel.Bridge and implement thier
 * own process
 * function
 */
public class MabsMetric {
  protected static final BufferAllocator allocator = new RootAllocator();
  protected static final Logger LOG = LoggerFactory.getLogger(MabsMetric.class);
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
        System.out.println(e.getMessage());
        LOG.error("fail to run MabsMetric", e);
      } finally {
        if (input != null)
          input.close();
        if (output != null)
          output.close();
      }
    }
  }

  static class ArrowCollector implements Collector<MabsBaseMetric> {
    public int count = 0;

    VarCharVector json_out;
    VarCharVector key;
    BigIntVector timestamp_out;

    public ArrowCollector(VarCharVector key, BigIntVector timestamp_out, VarCharVector json_out) {
      this.key = key;
      this.timestamp_out = timestamp_out;
      this.json_out = json_out;
    }

    @Override
    public void collect(MabsBaseMetric mabsBaseMetric) {
      try {
        String key_val = mabsBaseMetric.name + "#" + mabsBaseMetric.tags + "#" + mabsBaseMetric.metricType.toString();
        String payload = mapper.writeValueAsString(mabsBaseMetric);
        key.setSafe(count, key_val.getBytes(StandardCharsets.UTF_8));
        timestamp_out.setSafe(count, mabsBaseMetric.timestamp);
        json_out.setSafe(count, payload.getBytes(StandardCharsets.UTF_8));
        count++;
      } catch (Exception e) {
        LOG.error("fail to collect MabsBaseMetric", e);
      }
    }

    @Override
    public void close() {

    }
  }

  protected static VectorSchemaRoot process(VectorSchemaRoot input) throws Exception {
    VarCharVector key_out = new VarCharVector("key", allocator);
    BigIntVector timestamp_out = new BigIntVector("timestamp", allocator);
    VarCharVector json_out = new VarCharVector("json", allocator);
    key_out.allocateNew();
    timestamp_out.allocateNew();
    json_out.allocateNew();
    FlattenInputMabsMetrics s = new FlattenInputMabsMetrics();
    ArrowCollector ac = new ArrowCollector(key_out, timestamp_out, json_out);

    BigIntVector timestamp = (BigIntVector) input.getVector("timestamp");
    VarCharVector service_tags = (VarCharVector) input.getVector("service_tags");
    VarCharVector node_tags = (VarCharVector) input.getVector("node_tags");
    MapVector counters = (MapVector) input.getVector("counters");
    MapVector gauges = (MapVector) input.getVector("gauges");
    MapVector histograms = (MapVector) input.getVector("histograms");
    VarCharVector service_name = (VarCharVector) input.getVector("service_name");
    MapVector double_counters = (MapVector) input.getVector("double_counters");

    for (int i = 0; i < input.getRowCount(); i++) {
      MabsMetrics m = new MabsMetrics();
      try{
        m.timestamp = timestamp.get(i);
        m.service_tags = new Text(service_tags.get(i)).toString();
        m.node_tags = new Text(node_tags.get(i)).toString();

        UnionMapReader mapReader = counters.getReader();
        for (int k = 0; k < counters.getInnerValueCount() ; k++) {
          mapReader.next();
          String key = mapReader.key().readText().toString();
          Long val = mapReader.value().readLong();
          m.counters.put(key, val);
        }

        mapReader = gauges.getReader();
        for (int k = 0; k < gauges.getInnerValueCount() ; k++) {
          mapReader.next();
          String key = mapReader.key().readText().toString();
          double val = mapReader.value().readFloat();
          m.gauge.put(key, val);
        }

        mapReader = histograms.getReader();
        for (int k = 0; k < histograms.getInnerValueCount() ; k++) {
          mapReader.next();
          String key = mapReader.key().readText().toString();
          String val = mapReader.value().isSet() ? mapReader.value().readText().toString() : null;
          m.hist.put(key, val);
        }

        m.service_name = new Text(service_name.get(i)).toString();

        mapReader = double_counters.getReader();
        for (int k = 0; k < double_counters.getInnerValueCount() ; k++) {
          mapReader.next();
          String key = mapReader.key().readText().toString();
          double val = mapReader.value().readFloat();
          m.doubleconunters.put(key, val);
        }
        s.flatMap(m, ac);
      } catch (Exception e) {
        System.out.println(e.getMessage());
        LOG.error("fail to run flatMap", e);
      }
    }
    key_out.setValueCount(ac.count);
    json_out.setValueCount(ac.count);

    List<FieldVector> vectors = Arrays.asList(key_out, timestamp_out, json_out);
    VectorSchemaRoot vectorSchemaRoot = new VectorSchemaRoot(vectors);
    vectorSchemaRoot.setRowCount(ac.count);
    return vectorSchemaRoot;
  }
}
