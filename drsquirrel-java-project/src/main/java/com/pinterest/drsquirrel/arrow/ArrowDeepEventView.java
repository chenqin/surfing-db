package com.pinterest.drsquirrel.arrow;

import com.pinterest.deep.bench.Attr;
import com.pinterest.deep.bench.Bundle;
import com.pinterest.deep.bench.Geo;
import com.pinterest.deep.bench.Meta;
import com.pinterest.deep.bench.Reading;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.MapVector;
import org.apache.arrow.vector.complex.StructVector;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Arrow-backed implementation of {@link DeepEventView} that lazily materializes values
 * from a {@link VectorSchemaRoot} row.
 */
public final class ArrowDeepEventView implements DeepEventView {

  private final VectorSchemaRoot root;
  private final int row;

  private List<List<Integer>> metrics;
  private Set<Long> labelIds;
  private Map<String, List<Long>> countsByKey;
  private Meta meta;
  private List<Reading> readings;
  private Map<String, Bundle> bundles;
  private List<Map<String, List<Attr>>> attrMaps;
  private Geo geo;

  public ArrowDeepEventView(VectorSchemaRoot root, int row) {
    this.root = root;
    this.row = row;
  }

  @Override
  public long eventId() {
    BigIntVector vector = (BigIntVector) root.getVector("event_id");
    return vector.isNull(row) ? 0L : vector.get(row);
  }

  @Override
  public String source() {
    VarCharVector vector = (VarCharVector) root.getVector("source");
    return vector == null || vector.isNull(row)
        ? null
        : new String(vector.get(row), StandardCharsets.UTF_8);
  }

  @Override
  public List<List<Integer>> metrics() {
    if (metrics != null) return metrics;
    ListVector outer = (ListVector) root.getVector("metrics");
    if (outer == null || outer.isNull(row)) {
      metrics = Collections.emptyList();
      return metrics;
    }
    ListVector inner = (ListVector) outer.getDataVector();
    IntVector ints = (IntVector) inner.getDataVector();
    int start = outer.getElementStartIndex(row);
    int end = outer.getElementEndIndex(row);
    List<List<Integer>> out = new ArrayList<>(Math.max(0, end - start));
    for (int outerIdx = start; outerIdx < end; outerIdx++) {
      int innerStart = inner.getElementStartIndex(outerIdx);
      int innerEnd = inner.getElementEndIndex(outerIdx);
      List<Integer> innerList = new ArrayList<>(Math.max(0, innerEnd - innerStart));
      for (int pos = innerStart; pos < innerEnd; pos++) {
        innerList.add(ints.get(pos));
      }
      out.add(innerList);
    }
    metrics = Collections.unmodifiableList(out);
    return metrics;
  }

  @Override
  public Set<Long> labelIds() {
    if (labelIds != null) return labelIds;
    ListVector list = (ListVector) root.getVector("label_ids");
    if (list == null || list.isNull(row)) {
      labelIds = Collections.emptySet();
      return labelIds;
    }
    BigIntVector data = (BigIntVector) list.getDataVector();
    int start = list.getElementStartIndex(row);
    int end = list.getElementEndIndex(row);
    Set<Long> out = new HashSet<>(Math.max(0, end - start));
    for (int i = start; i < end; i++) {
      out.add(data.get(i));
    }
    labelIds = Collections.unmodifiableSet(out);
    return labelIds;
  }

  @Override
  public Map<String, List<Long>> countsByKey() {
    if (countsByKey != null) return countsByKey;
    MapVector mapVector = (MapVector) root.getVector("counts_by_key");
    if (mapVector == null || mapVector.isNull(row)) {
      countsByKey = Collections.emptyMap();
      return countsByKey;
    }
    StructVector entries = (StructVector) mapVector.getDataVector();
    VarCharVector keyVec = (VarCharVector) entries.getChild("key");
    ListVector valueVec = (ListVector) entries.getChild("value");
    BigIntVector longs = (BigIntVector) valueVec.getDataVector();
    int start = mapVector.getElementStartIndex(row);
    int end = mapVector.getElementEndIndex(row);
    Map<String, List<Long>> out = new HashMap<>();
    for (int i = start; i < end; i++) {
      String key = new String(keyVec.get(i), StandardCharsets.UTF_8);
      int valueStart = valueVec.getElementStartIndex(i);
      int valueEnd = valueVec.getElementEndIndex(i);
      List<Long> values = new ArrayList<>(Math.max(0, valueEnd - valueStart));
      for (int pos = valueStart; pos < valueEnd; pos++) values.add(longs.get(pos));
      out.put(key, values);
    }
    countsByKey = Collections.unmodifiableMap(out);
    return countsByKey;
  }

  @Override
  public Meta meta() {
    if (meta != null) return meta;
    StructVector metaVector = (StructVector) root.getVector("meta");
    if (metaVector == null || metaVector.isNull(row)) {
      meta = null;
      return meta;
    }
    Meta m = new Meta();
    MapVector labels = (MapVector) metaVector.getChild("labels");
    if (labels != null && !labels.isNull(row)) {
      Map<String, String> map = new HashMap<>();
      StructVector entries = (StructVector) labels.getDataVector();
      VarCharVector keyVec = (VarCharVector) entries.getChild("key");
      VarCharVector valueVec = (VarCharVector) entries.getChild("value");
      int start = labels.getElementStartIndex(row);
      int end = labels.getElementEndIndex(row);
      for (int i = start; i < end; i++) {
        String key = new String(keyVec.get(i), StandardCharsets.UTF_8);
        String val = new String(valueVec.get(i), StandardCharsets.UTF_8);
        map.put(key, val);
      }
      m.labels = map;
    }
    ListVector kvs = (ListVector) metaVector.getChild("kvs");
    if (kvs != null && !kvs.isNull(row)) {
      StructVector attrStruct = (StructVector) kvs.getDataVector();
      VarCharVector keyVec = (VarCharVector) attrStruct.getChild("key");
      VarCharVector valVec = (VarCharVector) attrStruct.getChild("val");
      int start = kvs.getElementStartIndex(row);
      int end = kvs.getElementEndIndex(row);
      List<Attr> attrs = new ArrayList<>(Math.max(0, end - start));
      for (int i = start; i < end; i++) {
        Attr attr = new Attr();
        attr.key = new String(keyVec.get(i), StandardCharsets.UTF_8);
        attr.val = new String(valVec.get(i), StandardCharsets.UTF_8);
        attrs.add(attr);
      }
      m.kvs = attrs;
    }
    meta = m;
    return meta;
  }

  @Override
  public List<Reading> readings() {
    if (readings != null) return readings;
    ListVector list = (ListVector) root.getVector("readings");
    if (list == null || list.isNull(row)) {
      readings = Collections.emptyList();
      return readings;
    }
    StructVector struct = (StructVector) list.getDataVector();
    BigIntVector tsVec = (BigIntVector) struct.getChild("ts");
    Float8Vector valueVec = (Float8Vector) struct.getChild("value");
    ListVector notesVec = (ListVector) struct.getChild("notes");
    VarCharVector notesData = notesVec == null ? null : (VarCharVector) notesVec.getDataVector();
    int start = list.getElementStartIndex(row);
    int end = list.getElementEndIndex(row);
    List<Reading> out = new ArrayList<>(Math.max(0, end - start));
    for (int i = start; i < end; i++) {
      Reading reading = new Reading();
      reading.ts = tsVec.get(i);
      reading.value = valueVec.get(i);
      if (notesVec != null && !notesVec.isNull(i)) {
        int nStart = notesVec.getElementStartIndex(i);
        int nEnd = notesVec.getElementEndIndex(i);
        List<String> notes = new ArrayList<>(Math.max(0, nEnd - nStart));
        for (int pos = nStart; pos < nEnd; pos++) {
          notes.add(new String(notesData.get(pos), StandardCharsets.UTF_8));
        }
        reading.notes = notes;
      }
      out.add(reading);
    }
    readings = Collections.unmodifiableList(out);
    return readings;
  }

  @Override
  public Map<String, Bundle> bundles() {
    if (bundles != null) return bundles;
    MapVector mapVector = (MapVector) root.getVector("bundles");
    if (mapVector == null || mapVector.isNull(row)) {
      bundles = Collections.emptyMap();
      return bundles;
    }
    StructVector entries = (StructVector) mapVector.getDataVector();
    VarCharVector keyVec = (VarCharVector) entries.getChild("key");
    ListVector valueVec = (ListVector) entries.getChild("value");
    StructVector bundleStruct = (StructVector) valueVec.getDataVector();
    ListVector itemsVec = (ListVector) bundleStruct.getChild("items");
    StructVector itemStruct = itemsVec == null ? null : (StructVector) itemsVec.getDataVector();
    BigIntVector tsVec = itemStruct == null ? null : (BigIntVector) itemStruct.getChild("ts");
    Float8Vector valVec = itemStruct == null ? null : (Float8Vector) itemStruct.getChild("value");
    Map<String, Bundle> out = new HashMap<>();
    int start = mapVector.getElementStartIndex(row);
    int end = mapVector.getElementEndIndex(row);
    for (int i = start; i < end; i++) {
      String key = new String(keyVec.get(i), StandardCharsets.UTF_8);
      Bundle bundle = new Bundle();
      if (itemsVec != null && !itemsVec.isNull(i)) {
        int itemStart = itemsVec.getElementStartIndex(i);
        int itemEnd = itemsVec.getElementEndIndex(i);
        List<Reading> itemReadings = new ArrayList<>(Math.max(0, itemEnd - itemStart));
        for (int pos = itemStart; pos < itemEnd; pos++) {
          Reading reading = new Reading();
          reading.ts = tsVec.get(pos);
          reading.value = valVec.get(pos);
          itemReadings.add(reading);
        }
        bundle.items = itemReadings;
      }
      MapVector extrasVec = (MapVector) bundleStruct.getChild("extras");
      if (extrasVec != null && !extrasVec.isNull(i)) {
        StructVector extraEntries = (StructVector) extrasVec.getDataVector();
        VarCharVector extraKey = (VarCharVector) extraEntries.getChild("key");
        ListVector extraValue = (ListVector) extraEntries.getChild("value");
        VarCharVector extraValueData = (VarCharVector) extraValue.getDataVector();
        int extraStart = extrasVec.getElementStartIndex(i);
        int extraEnd = extrasVec.getElementEndIndex(i);
        Map<String, List<String>> extras = new HashMap<>();
        for (int pos = extraStart; pos < extraEnd; pos++) {
          String eKey = new String(extraKey.get(pos), StandardCharsets.UTF_8);
          int valStart = extraValue.getElementStartIndex(pos);
          int valEnd = extraValue.getElementEndIndex(pos);
          List<String> values = new ArrayList<>(Math.max(0, valEnd - valStart));
          for (int idx = valStart; idx < valEnd; idx++) {
            values.add(new String(extraValueData.get(idx), StandardCharsets.UTF_8));
          }
          extras.put(eKey, values);
        }
        bundle.extras = extras;
      }
      out.put(key, bundle);
    }
    bundles = Collections.unmodifiableMap(out);
    return bundles;
  }

  @Override
  public List<Map<String, List<Attr>>> attrMaps() {
    if (attrMaps != null) return attrMaps;
    ListVector outer = (ListVector) root.getVector("attr_maps");
    if (outer == null || outer.isNull(row)) {
      attrMaps = Collections.emptyList();
      return attrMaps;
    }
    MapVector mapVector = (MapVector) outer.getDataVector();
    StructVector entryStruct = (StructVector) mapVector.getDataVector();
    VarCharVector keyVec = (VarCharVector) entryStruct.getChild("key");
    ListVector valueVec = (ListVector) entryStruct.getChild("value");
    StructVector attrStruct = (StructVector) valueVec.getDataVector();
    VarCharVector attrKey = (VarCharVector) attrStruct.getChild("key");
    VarCharVector attrVal = (VarCharVector) attrStruct.getChild("val");
    int outerStart = outer.getElementStartIndex(row);
    int outerEnd = outer.getElementEndIndex(row);
    List<Map<String, List<Attr>>> outerList = new ArrayList<>(Math.max(0, outerEnd - outerStart));
    for (int i = outerStart; i < outerEnd; i++) {
      Map<String, List<Attr>> map = new HashMap<>();
      int mapStart = mapVector.getElementStartIndex(i);
      int mapEnd = mapVector.getElementEndIndex(i);
      for (int entry = mapStart; entry < mapEnd; entry++) {
        String key = new String(keyVec.get(entry), StandardCharsets.UTF_8);
        int attrStart = valueVec.getElementStartIndex(entry);
        int attrEnd = valueVec.getElementEndIndex(entry);
        List<Attr> attrs = new ArrayList<>(Math.max(0, attrEnd - attrStart));
        for (int idx = attrStart; idx < attrEnd; idx++) {
          Attr attr = new Attr();
          attr.key = new String(attrKey.get(idx), StandardCharsets.UTF_8);
          attr.val = new String(attrVal.get(idx), StandardCharsets.UTF_8);
          attrs.add(attr);
        }
        map.put(key, attrs);
      }
      outerList.add(map);
    }
    attrMaps = Collections.unmodifiableList(outerList);
    return attrMaps;
  }

  @Override
  public Geo geo() {
    if (geo != null) return geo;
    StructVector geoVector = (StructVector) root.getVector("geo");
    if (geoVector == null || geoVector.isNull(row)) {
      geo = null;
      return geo;
    }
    Geo g = new Geo();
    Float8Vector lat = (Float8Vector) geoVector.getChild("lat");
    Float8Vector lon = (Float8Vector) geoVector.getChild("lon");
    StructVector region = (StructVector) geoVector.getChild("region");
    g.lat = lat.get(row);
    g.lon = lon.get(row);
    if (region != null && !region.isNull(row)) {
      com.pinterest.deep.bench.Region reg = new com.pinterest.deep.bench.Region();
      VarCharVector country = (VarCharVector) region.getChild("country");
      VarCharVector city = (VarCharVector) region.getChild("city");
      reg.country = country == null || country.isNull(row)
          ? null
          : new String(country.get(row), StandardCharsets.UTF_8);
      reg.city = city == null || city.isNull(row)
          ? null
          : new String(city.get(row), StandardCharsets.UTF_8);
      g.region = reg;
    }
    geo = g;
    return geo;
  }

  @Override
  public void close() {
    root.close();
  }
}
