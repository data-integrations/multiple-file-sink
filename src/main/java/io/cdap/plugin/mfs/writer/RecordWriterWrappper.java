package io.cdap.plugin.mfs.writer;

import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.plugin.mfs.filter.RecordFilter;
import io.cdap.plugin.mfs.formatter.RecordFormatter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * Class description here.
 */
public final class RecordWriterWrappper extends RecordWriter<NullWritable, StructuredRecord> {
  private final RecordFilter filter;
  private final RecordFormatter formatter;
  private final RecordWriter writer;
  private final boolean writeValueAsKey;

  public RecordWriterWrappper(boolean writeValueAsKey, RecordWriter writer, RecordFilter filter,
                              RecordFormatter formatter)
    throws IOException, InterruptedException {
    this.writeValueAsKey = writeValueAsKey;
    this.writer = writer;
    this.filter = filter;
    this.formatter = formatter;
  }

  @Override
  public void write(NullWritable nullWritable, StructuredRecord record)
    throws IOException, InterruptedException {
    if (filter.filter(record)) {
      if (!writeValueAsKey) {
        writer.write(NullWritable.get(), formatter.format(record));
      } else {
        writer.write(formatter.format(record), NullWritable.get());
      }
    }
  }

  @Override
  public void close(TaskAttemptContext ctx)
    throws IOException, InterruptedException {
    writer.close(ctx);
  }
}
