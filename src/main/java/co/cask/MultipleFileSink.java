package co.cask;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.writer.MultipleFileOutputProvider;
import org.apache.hadoop.io.NullWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name(MultipleFileSink.NAME)
@Description(MultipleFileSink.DESC)
public class MultipleFileSink extends BatchSink<StructuredRecord, NullWritable, StructuredRecord> {
  private static final Logger LOG =LoggerFactory.getLogger(MultipleFileSink.class);
  public static final String NAME = "MultipleFileSink";
  public static final String DESC = "Writes incoming records to multiple files.";
  private MultipleFileConfig config;

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {
    for (int i = 0; i < config.getFiltersAsList().length; ++i) {
      String id = String.format("%s-%d", config.getPrefix(), i);
      context.addOutput(
        Output.of(id, new MultipleFileOutputProvider(i, config.getFiltersAsList().length, config, context))
      );
    }
  }

  @Override
  public void transform(StructuredRecord input, Emitter<KeyValue<NullWritable,
    StructuredRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(NullWritable.get(), input));
  }
}
