package co.cask.formatter;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Class description here.
 */
public class CSVFormatter extends RecordFormatter<Text> {
  private static final Logger LOG = LoggerFactory.getLogger(CSVFormatter.class);
  private String delimiter;

  @Override
  public void configure() throws IOException {
    this.delimiter = getConfiguration().get("delimiter", ",");
  }

  @Override
  public Text format(StructuredRecord record) {
    Text text = new Text();
    StringBuilder line = new StringBuilder();
    for (Schema.Field field : record.getSchema().getFields()) {
      String fieldName = field.getName();
      Object fieldVal = record.get(fieldName);
      String fieldValStr = fieldVal == null ? "" : fieldVal.toString();
      line.append(fieldValStr).append(delimiter);
    }
    line.deleteCharAt(line.length() - 1);
    text.set(line.toString());
    return text;
  }
}
