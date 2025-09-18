package com.pinterest.drsquirrel.mabsutils;

import com.tdunning.math.stats.TDigest;
import java.io.Serializable;

public class MabsHistogram implements Serializable {
  public double sum;
  public double min;
  public double max;
  public long count;
  public TDigest digest;

  public MabsHistogram(double sum, double min, double max, long count, TDigest digest) {
    this.sum = sum;
    this.min = min;
    this.max = max;
    this.count = count;
    this.digest = digest;
  }
}
