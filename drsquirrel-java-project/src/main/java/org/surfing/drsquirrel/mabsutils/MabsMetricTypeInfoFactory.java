package org.surfing.drsquirrel.mabsutils;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import org.apache.flink.api.common.typeinfo.TypeInfoFactory;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;

public class MabsMetricTypeInfoFactory extends TypeInfoFactory<MabsBaseMetric> {
  @Override
  public TypeInformation<MabsBaseMetric> createTypeInfo(Type t,
                                                        Map<String, TypeInformation<?>>
                                                            genericParameters) {
    return Types.POJO(
        MabsBaseMetric.class,
        new HashMap<String, TypeInformation<?>>() {
          {
            put("name", Types.STRING);
            put("tags", Types.STRING);
            put("timestamp", Types.LONG);
            put("metricType", TypeInformation.of(MabsMetricType.class));
            put("counterValue", Types.LONG);
            put("counterMaxValue", Types.LONG);
            put("counterMinValue", Types.LONG);
            put("counterMetricCount", Types.LONG);
            put("doubleCounterValue", Types.DOUBLE);
            put("doubleCounterMaxValue", Types.DOUBLE);
            put("doubleCounterMinValue", Types.DOUBLE);
            put("doubleCounterMetricCount", Types.LONG);
            put("gaugeValue", Types.DOUBLE);
            put("gaugeMaxValue", Types.DOUBLE);
            put("gaugeMinValue", Types.DOUBLE);
            put("gaugeSumValue", Types.DOUBLE);
            put("gaugeMetricCount", Types.LONG);
            put("mabsHistogram", TypeInformation.of(MabsHistogram.class));
          }
        });
  }
}