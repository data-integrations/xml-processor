package io.cdap.plugins.xml;

import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.plugin.common.KeyValueListParser;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class DataMasker {
  public static final String DESCRIPTION = "A map from the path of the field to the unix sed expression to be " +
    "applied to values of that field. Inner record field names are separated by '.'. For example path of " +
    "'person.name' corresponds to the name field inside the person record field. Note that all the paths should " +
    "point to a string field type. If a field is not found or if the field is not of string type, " +
    "that operation is ignored";

  private final List<MaskInfo> dataMasks = new ArrayList<>();
  private final KeyValueListParser kvParser = new KeyValueListParser("\\s*,\\s*", ":");

  public DataMasker(String dataMasksString) {
    if (!Strings.isNullOrEmpty(dataMasksString)) {
      Iterable<KeyValue<String, String>> kvIterable = kvParser.parse(dataMasksString);
      populateMaskInfos(kvIterable);
    }
  }

  public List<MaskInfo> getDataMasks() {
    return dataMasks;
  }

  private void populateMaskInfos(Iterable<KeyValue<String, String>> dataMaskIterable) {
    Iterator<KeyValue<String, String>> kvIterator = dataMaskIterable.iterator();
    while (kvIterator.hasNext()) {
      KeyValue<String, String> mask = kvIterator.next();
      MaskInfo maskInfo = new MaskInfo(mask.getKey(), mask.getValue());
      dataMasks.add(maskInfo);
    }
  }
}
