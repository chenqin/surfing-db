package com.pinterest.drsquirrel.mabsutils;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import org.apache.flink.api.common.typeinfo.TypeInfo;

@TypeInfo(MabsMetricTypeInfoFactory.class)
public class MabsBaseMetric implements Serializable {
  public String name;
  public String tags;
  public Long timestamp;
  public MabsMetricType metricType = MabsMetricType.NONE;
  public Long counterValue;
  public Long counterMaxValue;
  public Long counterMinValue;
  public Long counterMetricCount = 0L;

  public Double doubleCounterValue;
  public Double doubleCounterMaxValue;
  public Double doubleCounterMinValue;
  public Long doubleCounterMetricCount = 0L;

  public Double gaugeValue;
  public Double gaugeMaxValue;
  public Double gaugeMinValue;
  public Double gaugeSumValue;
  public Long gaugeMetricCount = 0L;

  public MabsHistogram mabsHistogram;

  public MabsBaseMetric() {
  }

  private MabsBaseMetric(String name, String tags, Long timestamp, Long counterValue) {
    this.name = name;
    this.tags = tags;
    this.timestamp = timestamp;
    this.counterValue = counterValue;
    this.gaugeValue = null;
    this.mabsHistogram = null;
    this.doubleCounterValue = null;
    this.metricType = MabsMetricType.COUNTER;
  }

  private MabsBaseMetric(String name, String tags, Long timestamp, Double value, boolean isGauge) {
    this.name = name;
    this.tags = tags;
    this.timestamp = timestamp;
    this.counterValue = null;
    this.mabsHistogram = null;
    if (isGauge) {
      this.metricType = MabsMetricType.GAUGE;
      this.gaugeValue = value;
      this.doubleCounterValue = null;
    } else {
      this.metricType = MabsMetricType.DOUBLE_COUNTER;
      this.doubleCounterValue = value;
      this.gaugeValue = null;
    }
  }

  private MabsBaseMetric(String name, String tags, Long timestamp, MabsHistogram mabsHistogram) {
    this.name = name;
    this.tags = tags;
    this.timestamp = timestamp;
    this.gaugeValue = null;
    this.counterValue = null;
    this.doubleCounterValue = null;
    this.mabsHistogram = mabsHistogram;
    this.metricType = MabsMetricType.HISTOGRAM;
  }

  public static MabsBaseMetric createCounter(String name, String tags, Long timestamp,
                                             Long counterValue) {
    return new MabsBaseMetric(name, tags, timestamp, counterValue);
  }

  public static MabsBaseMetric createDoubleCounter(
      String name, String tags, Long timestamp, Double counterValue) {
    return new MabsBaseMetric(name, tags, timestamp, counterValue, false);
  }

  public static MabsBaseMetric createGauge(String name, String tags, Long timestamp,
                                           Double gaugeValue) {
    return new MabsBaseMetric(name, tags, timestamp, gaugeValue, true);
  }

  public static MabsBaseMetric createHistogram(String name, String tags, Long timestamp,
                                               MabsHistogram mabsHistogram) {
    return new MabsBaseMetric(name, tags, timestamp, mabsHistogram);
  }


  public List<String> getPutFormat() {
    switch (metricType) {
      case HISTOGRAM:
        return getHistogramPutFormat();
      case GAUGE:
        return getGaugePutFormat();
      case COUNTER:
        return getCounterPutFormat();
      case DOUBLE_COUNTER:
        return getDoubleCounterPutFormat();
    }
    return new ArrayList<>();
  }

  public static String getPutRecord(String name, long timestamp, String value, String tags) {
    return "put " + name + " " + timestamp + " " + value + " " + tags;
  }

  private List<String> getHistogramPutFormat() {
    String prefix = "mabs.histograms." + this.name;

    return Arrays.asList(
        getPutRecord(prefix+".p50", timestamp,
            Double.toString(mabsHistogram.digest.quantile(0.50)), tags),
        getPutRecord(prefix+".p75", timestamp,
            Double.toString(mabsHistogram.digest.quantile(0.75)), tags),
        getPutRecord(prefix+".p90", timestamp,
            Double.toString(mabsHistogram.digest.quantile(0.90)), tags),
        getPutRecord(prefix+".p95", timestamp,
            Double.toString(mabsHistogram.digest.quantile(0.95)), tags),
        getPutRecord(prefix+".p99", timestamp,
            Double.toString(mabsHistogram.digest.quantile(0.99)), tags),
        getPutRecord(prefix+".p999", timestamp,
            Double.toString(mabsHistogram.digest.quantile(0.999)), tags),
        getPutRecord(prefix+".p9999", timestamp,
            Double.toString(mabsHistogram.digest.quantile(0.9999)), tags),

        getPutRecord(prefix+".sum", timestamp, Double.toString(mabsHistogram.sum), tags),
        getPutRecord(prefix+".count", timestamp, Double.toString(mabsHistogram.count),
            tags),
        getPutRecord(prefix+".avg", timestamp,
            Double.toString(mabsHistogram.sum / mabsHistogram.count), tags),
        getPutRecord(prefix+".max", timestamp, Double.toString(mabsHistogram.max), tags),
        getPutRecord(prefix+".min", timestamp, Double.toString(mabsHistogram.min), tags)
    );
  }

  private List<String> getGaugePutFormat() {
    List<String> list = new LinkedList<>();
    if (gaugeValue != null) {
      list.add(getPutRecord("mabs.gauges." + name, timestamp, String.valueOf(gaugeValue), tags));
    }
    list.add(
        getPutRecord("mabs.gauges." + name + ".last", timestamp, String.valueOf(gaugeValue), tags));
    if (this.gaugeMaxValue != null) {
      list.add(
          getPutRecord(
              "mabs_agg.gauges." + name + ".max", timestamp, String.valueOf(gaugeMaxValue), tags));
    }
    if (this.gaugeMinValue != null) {
      list.add(
          getPutRecord(
              "mabs_agg.gauges." + name + ".min", timestamp, String.valueOf(gaugeMinValue), tags));
    }
    if (this.gaugeSumValue != null) {
      list.add(
          getPutRecord(
              "mabs_agg.gauges." + name + ".sum", timestamp, String.valueOf(gaugeSumValue), tags));
    }
    if (this.gaugeMetricCount > 0 && gaugeSumValue != null) {
      String avgValue = String.valueOf((long) (gaugeSumValue / gaugeMetricCount));
      list.add(getPutRecord("mabs_agg.gauges." + name + ".avg", timestamp, avgValue, tags));
      list.add(
          getPutRecord(
              "mabs_agg.gauges." + name + ".count",
              timestamp,
              String.valueOf(gaugeMetricCount),
              tags));
    }
    return list;
  }

  private List<String> getCounterPutFormat() {
    List<String> list = new LinkedList<>();
    if (counterValue != null) {
      list.add(getPutRecord("mabs.counters." + name, timestamp, Long.toString(counterValue), tags));
      list.add(
          getPutRecord(
              "mabs_agg.counters." + name + ".sum", timestamp, Long.toString(counterValue), tags));
    }
    if (this.counterMaxValue != null) {
      list.add(
          getPutRecord(
              "mabs_agg.counters." + name + ".max",
              timestamp,
              String.valueOf(counterMaxValue),
              tags));
    }
    if (this.counterMinValue != null) {
      list.add(
          getPutRecord(
              "mabs_agg.counters." + name + ".min",
              timestamp,
              String.valueOf(counterMinValue),
              tags));
    }
    if (this.counterMetricCount > 0 && counterValue != null) {
      String avgValue = String.valueOf((counterValue / counterMetricCount));
      list.add(getPutRecord("mabs_agg.counters." + name + ".avg", timestamp, avgValue, tags));
      list.add(
          getPutRecord(
              "mabs_agg.counters." + name + ".count",
              timestamp,
              String.valueOf(counterMetricCount),
              tags));
    }
    return list;
  }

  private List<String> getDoubleCounterPutFormat() {
    List<String> list = new LinkedList<>();
    if (doubleCounterValue != null) {
      list.add(
          getPutRecord(
              "mabs.double_counters." + name,
              timestamp,
              Double.toString(doubleCounterValue),
              tags));

      list.add(
          getPutRecord(
              "mabs_agg.double_counters." + name + ".sum",
              timestamp,
              Double.toString(doubleCounterValue),
              tags));
    }
    if (this.doubleCounterMaxValue != null) {
      list.add(
          getPutRecord(
              "mabs_agg.double_counters." + name + ".max",
              timestamp,
              String.valueOf(doubleCounterMaxValue),
              tags));
    }
    if (this.counterMinValue != null) {
      list.add(
          getPutRecord(
              "mabs_agg.double_counters." + name + ".min",
              timestamp,
              String.valueOf(doubleCounterMaxValue),
              tags));
    }
    if (this.doubleCounterMetricCount > 0 && doubleCounterValue != null) {
      String avgValue = String.valueOf((long) (doubleCounterValue / doubleCounterMetricCount));
      list.add(
          getPutRecord("mabs_agg.double_counters." + name + ".avg", timestamp, avgValue, tags));
      list.add(
          getPutRecord(
              "mabs_agg.double_counters." + name + ".count",
              timestamp,
              String.valueOf(doubleCounterMetricCount),
              tags));
    }
    return list;
  }

  public MabsBaseMetric plus(MabsBaseMetric other) {
    if (this.metricType != other.metricType) {
      return null;
    }
    if (other.metricType == MabsMetricType.NONE) {
      return null;
    }

    switch (this.metricType) {
      case GAUGE:
        this.gaugeValue = other.gaugeValue;
        if (this.gaugeMaxValue == null) {
          this.gaugeMaxValue = other.gaugeValue;
        } else if (this.gaugeMaxValue < other.gaugeValue) {
          this.gaugeMaxValue = other.gaugeValue;
        }

        if (this.gaugeMinValue == null) {
          this.gaugeMinValue = other.gaugeValue;
        } else if (this.gaugeMinValue > other.gaugeValue) {
          this.gaugeMinValue = other.gaugeValue;
        }
        if (this.gaugeSumValue == null) {
          this.gaugeSumValue = other.gaugeValue;
        } else {
          this.gaugeSumValue += other.gaugeValue;
        }
        this.gaugeMetricCount += 1;
        return this;
      case COUNTER:
        this.counterValue = this.counterValue + other.counterValue;
        if (this.counterMaxValue == null) {
          this.counterMaxValue = other.counterValue;
        } else if (this.counterMaxValue < other.counterValue) {
          this.counterMaxValue = other.counterValue;
        }

        if (this.counterMinValue == null) {
          this.counterMinValue = other.counterValue;
        } else if (this.counterMinValue > other.counterValue) {
          this.counterMinValue = other.counterValue;
        }
        this.counterMetricCount += 1;
        return this;
      case DOUBLE_COUNTER:
        this.doubleCounterValue = this.doubleCounterValue + other.doubleCounterValue;
        if (this.doubleCounterMaxValue == null) {
          this.doubleCounterMaxValue = other.doubleCounterValue;
        } else if (this.doubleCounterMaxValue < other.doubleCounterValue) {
          this.doubleCounterMaxValue = other.doubleCounterValue;
        }

        if (this.doubleCounterMinValue == null) {
          this.doubleCounterMinValue = other.doubleCounterValue;
        } else if (this.doubleCounterMinValue > other.doubleCounterValue) {
          this.doubleCounterMinValue = other.doubleCounterValue;
        }
        this.doubleCounterMetricCount += 1;
        return this;
      case HISTOGRAM:
        if (this.mabsHistogram != null && other.mabsHistogram != null) {
          this.mabsHistogram.sum = this.mabsHistogram.sum + other.mabsHistogram.sum;
          this.mabsHistogram.min = Math.min(this.mabsHistogram.min, other.mabsHistogram.min);
          this.mabsHistogram.max = Math.max(this.mabsHistogram.max, other.mabsHistogram.max);
          this.mabsHistogram.count = this.mabsHistogram.count + other.mabsHistogram.count;
          if (this.mabsHistogram.digest != null && other.mabsHistogram.digest != null) {
            this.mabsHistogram.digest.add(other.mabsHistogram.digest);
          }
          return this;
        } else {
          return this;

        }
      default:
        return null;
    }
  }
}
