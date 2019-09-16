package io.cdap.plugin.mfs.formatter;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Map;

/**
 * Class description here.
 */
public abstract class RecordFormatter<O> {
  private int partitionId;
  private Map<String, String> arguments;
  private Configuration configuration;

  protected RecordFormatter() {
    // No-op
  }
  public void configure(int partitionId, Map<String, String> arguments, Configuration configuration)
    throws IOException {
    this.partitionId = partitionId;
    this.arguments = arguments;
    this.configuration = configuration;
    configure();
  }
  protected int getPartitionId() {
    return partitionId;
  }

  protected Map<String, String> getArguments() {
    return arguments;
  }

  protected Configuration getConfiguration() {
    return configuration;
  }
  public abstract void configure() throws IOException;
  public abstract O format(StructuredRecord record) throws IOException;
}
