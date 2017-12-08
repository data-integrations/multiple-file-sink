package co.cask.formatter;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.format.StructuredRecordStringConverter;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * Class description here.
 */
public class JsonFormatter extends RecordFormatter<Text> {
  @Override
  public void configure() throws IOException {
    // no-op
  }

  @Override
  public Text format(StructuredRecord record) throws IOException {
    return new Text(StructuredRecordStringConverter.toJsonString(record));
  }
}
