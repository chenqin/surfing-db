package org.surfing.drsquirrel.mabsutils;

import com.tdunning.math.stats.MergingDigest;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.Base64;
import org.apache.flink.api.java.tuple.Tuple2;

public class MabsUtils {


  public static Tuple2<String, String> extractMetricNameAndTags(String nameAndTags,
                                                                String appendedTags) {
    String fullKey = nameAndTags.trim();
    if (!appendedTags.equals("")) {
      fullKey = fullKey + " " + appendedTags;
    }

    int idxOfSpace = fullKey.indexOf(' ');
    if (idxOfSpace != -1) {
      String name = fullKey.substring(0, idxOfSpace);
      String tags = fullKey.substring(idxOfSpace + 1).trim();
      return Tuple2.of(name, tags);
    } else {
      return Tuple2.of("", "");
    }

  }

  public static MabsHistogram deserializeDistribution(byte[] serialized) {
    try {
      ByteBuffer bb = ByteBuffer.wrap(serialized);
      return new MabsHistogram(
          bb.getDouble(), // sum
          bb.getDouble(), // min
          bb.getDouble(), // max
          bb.getLong(), // count
          MergingDigest.fromBytes(bb) // digest
      );
    } catch (BufferUnderflowException e) {
      throw new RuntimeException("Not enough bytes in source histogram to decode fields, check"
          + "serialization source follows the spec defined in MabsUtils.serializeDistribution", e);
    } catch (IllegalStateException e) {
      throw new RuntimeException("Failed to deserialize TDigest, check serialization source follows "
          + "the spec defined in MabsUtils.serializeDistribution", e);
    }
  }

  public static MabsHistogram deserializeDistributionBase64(String serialized) {
    if(serialized == null) return null;
    return deserializeDistribution(Base64.getDecoder().decode(serialized));
  }

}
