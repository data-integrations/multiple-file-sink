package io.cdap.plugin.mfs.filter;

import io.cdap.cdap.api.data.format.StructuredRecord;

/**
 * Class description here.
 */
public class NoFilter extends RecordFilter {

  @Override
  public void configure() {
    // no-op
  }

  @Override
  public boolean filter(StructuredRecord record) {
    return false;
  }
}
