package org.surfing.drsquirrel.flink;

import org.surfing.deep.bench.Attr;
import org.surfing.deep.bench.Bundle;
import org.surfing.deep.bench.DeepEvent;
import org.surfing.deep.bench.Geo;
import org.surfing.deep.bench.Meta;
import org.surfing.deep.bench.Region;
import org.surfing.deep.bench.Reading;
import org.surfing.drsquirrel.arrow.ArrowDeepEventDecoder;
import org.surfing.drsquirrel.arrow.DeepEventView;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TMemoryBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Flink DataStream job showing how to feed Thrift payloads into surfingthriftjni and expose
 * Arrow-backed DeepEvent views to downstream operators.
 */
public final class DeepEventDataStreamExample {

  private static final int DEFAULT_EVENTS = 10_000;
  private static final long DEFAULT_EVENTS_PER_SECOND = 5_000L;
  private static final String THRIFT_PATH = "src/bench/deep_event.thrift";
  private static final String THRIFT_STRUCT = "DeepEvent";

  public static void main(String[] args) throws Exception {
    int totalEvents = Integer.parseInt(System.getProperty(
        "deep.event.count",
        Integer.toString(DEFAULT_EVENTS)));
    long rate = Long.parseLong(System.getProperty(
        "deep.event.rate",
        Long.toString(DEFAULT_EVENTS_PER_SECOND)));

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    DataGeneratorSource<Long> source = new DataGeneratorSource<>(
        SequenceGenerator.longGenerator(0L, Math.max(0, totalEvents - 1)),
        totalEvents,
        rate);

    DataStream<String> summaries = env
        .addSource(source, "deep-event-datagen")
        .map(new DeepEventThriftEncoder()).name("encode-deep-event")
        .map(new ArrowDeepEventDecoder(THRIFT_PATH, THRIFT_STRUCT)).name("arrow-decode")
        .map(new SummarizeDeepEvent()).name("summaries");

    summaries.print();

    env.execute("DeepEvent datagen (Arrow-backed)");
  }

  /** Encodes synthetic DeepEvent payloads as Thrift binary frames. */
  private static final class DeepEventThriftEncoder extends RichMapFunction<Long, byte[]> {
    @Override
    public byte[] map(Long id) throws Exception {
      DeepEvent event = DeepEventPayloadFixtures.buildPojo(id);
      TMemoryBuffer buffer = new TMemoryBuffer(4096);
      TBinaryProtocol proto = new TBinaryProtocol(buffer);
      event.write(proto);
      return java.util.Arrays.copyOf(buffer.getArray(), buffer.length());
    }
  }

  /** Summarises Arrow-backed DeepEvent views and releases underlying buffers. */
  private static final class SummarizeDeepEvent implements MapFunction<DeepEventView, String> {
    @Override
    public String map(DeepEventView view) throws Exception {
      try (DeepEventView v = view) {
        int readings = v.readings() == null ? 0 : v.readings().size();
        String region = v.meta() != null && v.meta().labels != null
            ? v.meta().labels.getOrDefault("region", "n/a")
            : "n/a";
        return String.format("event_id=%d source=%s readings=%d region=%s",
            v.eventId(),
            v.source(),
            readings,
            region);
      }
    }
  }

  /** Reusable fixture helpers mirrored from the JNI benchmark generator. */
  private static final class DeepEventPayloadFixtures {
    private static DeepEvent buildPojo(long id) {
      ThreadLocalRandom rnd = ThreadLocalRandom.current();
      DeepEvent event = new DeepEvent();
      event.event_id = 1_000_000L + id;
      event.source = "flink-src-" + (id % 16);
      event.metrics = buildMetrics(rnd);
      event.label_ids = buildLabelIds(rnd);
      event.counts_by_key = buildCountsByKey();
      event.meta = buildMeta();
      event.readings = buildReadings(id, rnd);
      event.bundles = buildBundles(id, rnd);
      event.attr_maps = buildAttrMaps();
      event.geo = buildGeo();
      return event;
    }

    private static List<List<Integer>> buildMetrics(ThreadLocalRandom rnd) {
      int outer = rnd.nextInt(1, 4);
      List<List<Integer>> metrics = new ArrayList<>(outer);
      for (int i = 0; i < outer; i++) {
        int inner = rnd.nextInt(1, 6);
        List<Integer> ints = new ArrayList<>(inner);
        for (int j = 0; j < inner; j++) ints.add(rnd.nextInt(1_000));
        metrics.add(ints);
      }
      return metrics;
    }

    private static Set<Long> buildLabelIds(ThreadLocalRandom rnd) {
      int size = rnd.nextInt(1, 6);
      Set<Long> set = new HashSet<>(size);
      for (int i = 0; i < size; i++) set.add(rnd.nextLong(1_000_000L));
      return set;
    }

    private static Map<String, List<Long>> buildCountsByKey() {
      Map<String, List<Long>> map = new HashMap<>();
      map.put("foo", java.util.Arrays.asList(1L, 2L, 3L));
      map.put("bar", Collections.singletonList(42L));
      return map;
    }

    private static Meta buildMeta() {
      Meta meta = new Meta();
      Map<String, String> labels = new HashMap<>();
      labels.put("region", "na");
      labels.put("env", "prod");
      meta.labels = labels;
      Attr attr = new Attr();
      attr.key = "k";
      attr.val = "v";
      meta.kvs = Collections.singletonList(attr);
      return meta;
    }

    private static List<Reading> buildReadings(long id, ThreadLocalRandom rnd) {
      List<Reading> readings = new ArrayList<>();
      int count = 1 + (int) (id % 4);
      for (int i = 0; i < count; i++) {
        Reading r = new Reading();
        r.ts = Instant.now().toEpochMilli() - i * 1_000L;
        r.value = rnd.nextDouble(0.0, 100.0);
        r.notes = new ArrayList<>();
        r.notes.add("note_" + i);
        readings.add(r);
      }
      return readings;
    }

    private static Map<String, Bundle> buildBundles(long id, ThreadLocalRandom rnd) {
      Map<String, Bundle> bundles = new HashMap<>();
      Bundle b = new Bundle();
      b.items = buildReadings(id, rnd);
      Map<String, List<String>> extras = new HashMap<>();
      extras.put("extra", Collections.singletonList("val"));
      b.extras = extras;
      bundles.put("bundle-" + id, b);
      return bundles;
    }

    private static List<Map<String, List<Attr>>> buildAttrMaps() {
      Map<String, List<Attr>> inner = new HashMap<>();
      Attr attr = new Attr();
      attr.key = "akey";
      attr.val = "aval";
      inner.put("attrs", Collections.singletonList(attr));
      return Collections.singletonList(inner);
    }

    private static Geo buildGeo() {
      Geo geo = new Geo();
      geo.lat = 34.0522;
      geo.lon = -118.2437;
      Region region = new Region();
      region.country = "US";
      region.city = "LA";
      geo.region = region;
      return geo;
    }
  }

  private DeepEventDataStreamExample() {}
}
