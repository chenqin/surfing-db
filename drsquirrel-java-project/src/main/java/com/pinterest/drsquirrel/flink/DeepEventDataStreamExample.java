package com.pinterest.drsquirrel.flink;

import com.pinterest.deep.bench.Attr;
import com.pinterest.deep.bench.Bundle;
import com.pinterest.deep.bench.DeepEvent;
import com.pinterest.deep.bench.Geo;
import com.pinterest.deep.bench.Meta;
import com.pinterest.deep.bench.Region;
import com.pinterest.deep.bench.Reading;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource;
import org.apache.flink.streaming.api.functions.source.datagen.SequenceGenerator;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Minimal Flink DataStream job that generates DeepEvent records via the built-in datagen source
 * and exercises the generated Thrift stub inside subsequent map functions.
 */
public final class DeepEventDataStreamExample {

  private static final int DEFAULT_EVENTS = 10_000;
  private static final long DEFAULT_EVENTS_PER_SECOND = 5_000L;

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

    DataStream<DeepEvent> events = env
        .fromSource(source, WatermarkStrategy.noWatermarks(), "deep-event-datagen")
        .map(new BuildDeepEvent(), "build-deep-event");

    events
        .map(new SummarizeDeepEvent(), "summaries")
        .print();

    env.execute("DeepEvent datagen DataStream");
  }

  private static final class BuildDeepEvent implements MapFunction<Long, DeepEvent> {
    @Override
    public DeepEvent map(Long id) {
      ThreadLocalRandom rnd = ThreadLocalRandom.current();
      DeepEvent event = new DeepEvent();
      event.event_id = 1_000_000L + id;
      event.source = "flink-src-" + (id % 16);
      event.metrics = buildMetrics(rnd);
      event.label_ids = buildLabelIds(rnd);
      event.counts_by_key = DeepEventPayloadFixtures.buildCountsByKey();
      event.meta = DeepEventPayloadFixtures.buildMeta();
      event.readings = DeepEventPayloadFixtures.buildReadings(id, rnd);
      event.bundles = DeepEventPayloadFixtures.buildBundles(id, rnd);
      event.attr_maps = DeepEventPayloadFixtures.buildAttrMaps();
      event.geo = DeepEventPayloadFixtures.buildGeo();
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
  }

  private static final class SummarizeDeepEvent implements MapFunction<DeepEvent, String> {
    @Override
    public String map(DeepEvent value) {
      int readings = value.readings == null ? 0 : value.readings.size();
      String region = value.meta != null && value.meta.region != null ? value.meta.region.name : "n/a";
      return String.format("event_id=%d source=%s readings=%d region=%s",
          value.event_id,
          value.source,
          readings,
          region);
    }
  }

  private static final class DeepEventPayloadFixtures {
    private static Meta buildMeta() {
      Meta meta = new Meta();
      meta.version = 1;
      meta.region = new Region();
      meta.region.name = "na";
      meta.region.country = "US";
      meta.region.lat = 37.7749;
      meta.region.lon = -122.4194;
      meta.created_at_ms = Instant.now().toEpochMilli();
      return meta;
    }

    private static List<Reading> buildReadings(long id, ThreadLocalRandom rnd) {
      List<Reading> readings = new ArrayList<>();
      int count = 1 + (int) (id % 4);
      for (int i = 0; i < count; i++) {
        Reading r = new Reading();
        r.ts = Instant.now().toEpochMilli() - i * 1_000L;
        r.metric = "m" + i;
        r.value = rnd.nextDouble(0.0, 100.0);
        readings.add(r);
      }
      return readings;
    }

    private static Map<String, Bundle> buildBundles(long id, ThreadLocalRandom rnd) {
      Map<String, Bundle> bundles = new HashMap<>();
      Bundle b = new Bundle();
      b.bundle_id = "b" + id;
      b.weight = rnd.nextDouble(1.0, 10.0);
      bundles.put(b.bundle_id, b);
      return bundles;
    }

    private static List<Map<String, List<Attr>>> buildAttrMaps() {
      List<Map<String, List<Attr>>> outer = new ArrayList<>();
      Map<String, List<Attr>> inner = new HashMap<>();
      inner.put("keys", java.util.Collections.singletonList(buildAttr("k", "v")));
      outer.add(inner);
      return outer;
    }

    private static Map<String, List<Long>> buildCountsByKey() {
      Map<String, List<Long>> map = new HashMap<>();
      map.put("foo", java.util.Arrays.asList(1L, 2L, 3L));
      map.put("bar", java.util.Collections.singletonList(42L));
      return map;
    }

    private static Geo buildGeo() {
      Geo geo = new Geo();
      geo.lat = 34.0522;
      geo.lon = -118.2437;
      geo.city = "LA";
      return geo;
    }

    private static Attr buildAttr(String key, String value) {
      Attr attr = new Attr();
      attr.key = key;
      attr.val = value;
      return attr;
    }
  }

  private DeepEventDataStreamExample() {}
}
