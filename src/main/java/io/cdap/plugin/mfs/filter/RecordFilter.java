package io.cdap.plugin.mfs.filter;

import io.cdap.cdap.api.data.format.StructuredRecord;
import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Class description here.
 */
public abstract class RecordFilter {
  private int partitionId;
  private Map<String, String> arguments;
  private Configuration configuration;

  public RecordFilter() {
    // Not allowerd
  }

  public void configure(int partitionId, Map<String, String> arguments, Configuration configuration)
    throws IllegalArgumentException {
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

  public abstract void configure();
  public abstract boolean filter(StructuredRecord record);
}
