package io.cdap.plugin.mfs;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Output;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSink;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.mfs.writer.MultipleFileOutputProvider;
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
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
    FailureCollector failureCollector = pipelineConfigurer.getStageConfigurer().getFailureCollector();
    config.validate(failureCollector, inputSchema);
    failureCollector.getOrThrowException();
  }

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
