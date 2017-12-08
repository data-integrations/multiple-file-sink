package co.cask.formatter;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.writer.StructuredToAvroTransformer;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * Class description here.
 */
public class GenericRecordFormatter extends RecordFormatter<GenericRecord> {
  private StructuredToAvroTransformer transformer;

  @Override
  public void configure() throws IOException {
    String schema = getConfiguration().get(DatasetProperties.SCHEMA);
    if (schema == null) {
      throw new IOException(
        String.format("Schema not specified for the formatter.")
      );
    }
    this.transformer = new StructuredToAvroTransformer(schema);
  }

  @Override
  public GenericRecord format(StructuredRecord record) throws IOException {
    return transformer.transform(record);
  }
}
