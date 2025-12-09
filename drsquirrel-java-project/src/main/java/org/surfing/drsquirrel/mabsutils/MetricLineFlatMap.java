package org.surfing.drsquirrel.mabsutils;

import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;

public class MetricLineFlatMap implements CheckpointedFunction,
                                          FlatMapFunction<MabsBaseMetric, String> {
  private static final long serialVersionUID = 1L;
  private List<String> bufferedMetrics;
  private transient ListState<String> checkpointedState;

  public MetricLineFlatMap() {
    this.bufferedMetrics = new ArrayList<>();
  }

  @Override
  public void flatMap(MabsBaseMetric metric, Collector<String> collector) throws Exception {
    for (String string: metric.getPutFormat()) {
      bufferedMetrics.add(string);
      if (bufferedMetrics.size() >= 250) {
        collector.collect(bufferedMetrics.toString());
        bufferedMetrics.clear();
      }
    }
  }

  @Override
  public void snapshotState(FunctionSnapshotContext context) throws Exception {
    checkpointedState.clear();
    for (String metric : bufferedMetrics) {
      checkpointedState.add(metric);
    }
  }

  @Override
  public void initializeState(FunctionInitializationContext context)
      throws Exception {
    ListStateDescriptor<String> descriptor =
        new ListStateDescriptor<>(
            "buffered-metrics",
            TypeInformation.of(new TypeHint<String>() {}));

    checkpointedState = context.getOperatorStateStore().getListState(descriptor);

    if (context.isRestored()) {
      for (String element : checkpointedState.get()) {
        bufferedMetrics.add(element);
      }
    }
  }
}
