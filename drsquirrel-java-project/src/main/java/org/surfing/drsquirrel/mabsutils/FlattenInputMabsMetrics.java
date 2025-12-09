package org.surfing.drsquirrel.mabsutils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FlattenInputMabsMetrics implements FlatMapFunction<MabsMetrics, MabsBaseMetric> {
  @Override
  public void flatMap(MabsMetrics mabsMetrics, Collector<MabsBaseMetric> collector)
      throws Exception {
    String appendedTags = mabsMetrics.getService_tags() != null ?
                          mabsMetrics.getService_tags().trim() : "";
    if (mabsMetrics.getCounters() != null) {
      mabsMetrics.getCounters().forEach((nameAndTags, value) -> {
        Tuple2<String, String>
            counterNameAndTags = MabsUtils.extractMetricNameAndTags(nameAndTags, appendedTags);
        collector.collect(MabsBaseMetric.createCounter(counterNameAndTags.f0, counterNameAndTags.f1,
            mabsMetrics.getTimestamp(), value));
      });
    }

    if (mabsMetrics.getDouble_counters() != null) {
      mabsMetrics
          .getDouble_counters()
          .forEach(
              (nameAndTags, value) -> {
                Tuple2<String, String> counterNameAndTags =
                    MabsUtils.extractMetricNameAndTags(nameAndTags, appendedTags);
                collector.collect(
                    MabsBaseMetric.createDoubleCounter(
                        counterNameAndTags.f0,
                        counterNameAndTags.f1,
                        mabsMetrics.getTimestamp(),
                        value));
              });
    }

    if (mabsMetrics.getGauges() != null) {
      mabsMetrics.getGauges().forEach((nameAndTags, value) -> {
        Tuple2<String, String>
            counterNameAndTags = MabsUtils.extractMetricNameAndTags(nameAndTags, appendedTags);
        collector.collect(MabsBaseMetric.createGauge(counterNameAndTags.f0, counterNameAndTags.f1,
            mabsMetrics.getTimestamp(), value));
      });
    }

    if (mabsMetrics.getHistograms() != null) {
      mabsMetrics.getHistograms().forEach((nameAndTags, value) -> {
        MabsHistogram histogram = MabsUtils.deserializeDistributionBase64(value);
        Tuple2<String, String>
            counterNameAndTags = MabsUtils.extractMetricNameAndTags(nameAndTags, appendedTags);
        MabsBaseMetric metric = MabsBaseMetric.createHistogram(counterNameAndTags.f0,
            counterNameAndTags.f1,
            mabsMetrics.getTimestamp(), histogram);
        collector.collect(metric);

      });
    }
  }
}