package com.pinterest.drsquirrel.arrow;

import com.pinterest.deep.bench.Attr;
import com.pinterest.deep.bench.Bundle;
import com.pinterest.deep.bench.Geo;
import com.pinterest.deep.bench.Meta;
import com.pinterest.deep.bench.Reading;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Minimal view over a DeepEvent payload. Implementations may be backed by Arrow vectors
 * and materialize values on demand.
 */
public interface DeepEventView extends AutoCloseable {

  long eventId();

  String source();

  List<List<Integer>> metrics();

  Set<Long> labelIds();

  Map<String, List<Long>> countsByKey();

  Meta meta();

  List<Reading> readings();

  Map<String, Bundle> bundles();

  List<Map<String, List<Attr>>> attrMaps();

  Geo geo();

  /** Optional hook for releasing backing resources. */
  @Override
  default void close() {}
}
